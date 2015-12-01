package zx.soft.kafka.consumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import zx.soft.kafka.hbase.HBaseConfig;
import zx.soft.kafka.hbase.HBaseTable;
import zx.soft.kafka.utils.ConvertUtil;

/**
 *
 * @author donglei
 *
 */
public class ConsumerRunnable implements Runnable {

	private static Logger logger = LoggerFactory.getLogger("KAFKA_LOG");

	private KafkaStream<byte[], byte[]> m_stream;

	private AtomicLong count;
	private MessageHandler handler;
	
	private ConcurrentHashMap<ByteWrapper, byte[]> _resultmap;
	
	private Object object;
	
	private int batchSize = 1000;
	
	private int msgCount = 0;
	
	// 从低位到高位
	private static byte[] remarkOfEndStream = new byte[] {100, 101, 103, 104, 105};
	
	// PCAP文件标示
	private static int lenOfPlaceholder = 2;
	
	private HBaseTable table;

	public ConsumerRunnable() {}
	
	public ConsumerRunnable(KafkaStream<byte[], byte[]> a_stream, AtomicLong count, MessageHandler handler, ConcurrentHashMap<ByteWrapper, byte[]> map, Object obj) {
		m_stream = a_stream;
		this.count = count;
		this.handler = handler;
		this._resultmap = map;
		this.object  = obj;
		
		try {
			this.table = new HBaseTable(HConnectionManager.createConnection(HBaseConfig.getZookeeperConf()), "kafka_db");
		} catch (IOException e) {
			logger.error("initial failed");
		}
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()) {
//			synchronized(object) {
				writeToBigDataAfter(it);
//			}
		}
		logger.info("Shutting down Thread: " + this.hashCode());
	}

	private void writeToBigDataAfter(ConsumerIterator<byte[], byte[]> it) {
		++msgCount;
		byte[] receivedMsg = it.next().message();
		this.handler.handleMessage(receivedMsg);
		count.incrementAndGet();
		try {
			table.put(convertToRowkey(receivedMsg),
					"load", convertToOrder(receivedMsg), convertToContent(receivedMsg));
			table.execute();
		} catch (Exception e) {
			logger.error("write to hbase error : ", e);
		}
	}

	private String convertToOrder(byte[] receivedMsg) {
		byte[] order = new byte[4];
		System.arraycopy(receivedMsg, 24, order, 0, 4);
		int result = ConvertUtil.fromByteArray(order);
		return Integer.toString(result);
	}

	private void printed(byte[] receivedMsg) {
		System.out.println(convertToRowkey(receivedMsg));
	}

	// 16 + 8 + 4 = 28
	private byte[] convertToContent(byte[] receivedMsg) {
		byte[] load = new byte[receivedMsg.length - 28];
		System.arraycopy(receivedMsg, 28, load, 0, receivedMsg.length - 28);
		return load;
	}

	private String convertToRowkey(byte[] receivedMsg) {
		StringBuilder sb = new StringBuilder();
		byte[] buffer = new byte[2];
		byte[] order = new byte[4];
		byte[] count = new byte[4];
		for (int i = 0; i < 24; i++) {
			buffer[i % 2] = receivedMsg[i];
			if (i % 2 == 1) {
				sb.append(Hex.encodeHexString(buffer));
			}
		}
		// 文件的顺序位置标示
//		System.arraycopy(receivedMsg, 24, order, 0, 4);
//		int ordered = ConvertUtil.fromByteArray(order);
//		if (ordered == -1) {
//			System.arraycopy(receivedMsg, 28, count, 0, 4);
//			sb.append('|').append(ordered).append('|').append(ConvertUtil.fromByteArray(count));			
//		} else {
//			sb.append('|').append(ordered);
//		}
		return sb.toString();
	}

	@SuppressWarnings("unused")
	private void writeToBigTableBefore(ConsumerIterator<byte[], byte[]> it) {
		byte[] receivedMsg = it.next().message();
		ByteWrapper key = new ByteWrapper(getFileRemarkkey(receivedMsg, lenOfPlaceholder));
		byte[] current = getCucurrentValue(receivedMsg, lenOfPlaceholder);
		if (_resultmap.get(key) != null) {
			byte[] result = combineTwobytes(_resultmap.get(key), current);
			_resultmap.put(key, result);
		} else {
			_resultmap.put(key, current);
		}
		if (isMatchEndRemark(receivedMsg, remarkOfEndStream, lenOfPlaceholder)) {
			writeToBigData(_resultmap.get(key));
			_resultmap.remove(key);
		}
		this.handler.handleMessage(receivedMsg);
		count.incrementAndGet();	
	}

	private boolean isMatchEndRemark(byte[] receivedMsg, byte[] bytes, int length) {
		int length1 = receivedMsg.length;
		int length2 = bytes.length;
		if (length1 < length2 + length) {
			return false;
		}
		byte[] result = new byte[length2];
		System.arraycopy(receivedMsg, length1 - length - length2, result, 0, length2);
		return Arrays.equals(result, bytes);
	}

	private byte[] combineTwobytes(byte[] data1, byte[] data2) {
		byte[] result = new byte[data1.length + data2.length];
		System.arraycopy(data1, 0, result, 0, data1.length);
		System.arraycopy(data2, 0, result, data1.length, data2.length);
		return result;
	}

	private void writeToBigData(byte[] result) {
		logger.debug("The complete stream is : " + new String(result));
	}

	private byte[] getCucurrentValue(byte[] receivedMsg, int length) {
		byte[] newbytes = new byte[receivedMsg.length - length];
		System.arraycopy(receivedMsg, 0, newbytes, 0, receivedMsg.length - length);
		return newbytes;
	}

	private byte[] getFileRemarkkey(byte[] receivedMsg, int length) {
		byte[] key  = new byte[length];
		System.arraycopy(receivedMsg, receivedMsg.length - length, key, 0, length);
		return key;
	}

	public static void main(String[] args) {
		ConsumerRunnable r = new ConsumerRunnable();
		byte[] receivedMsg = new byte[] {0x20, 0x01, 0x0d, (byte) 0xb8, (byte) 0x85, (byte) 0xa3, 0x08, (byte) 0xd3,
				0x13, 0x19, (byte) 0x8a, 0x2e, 0x03, 0x70, 0x73, 0x44,
				0x00, 0x00, 0x00, 0x01,
				(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
		long before = System.currentTimeMillis();
		System.out.println(r.convertToRowkey(receivedMsg));
		long after = System.currentTimeMillis();
		System.out.println(after - before);
	}
}