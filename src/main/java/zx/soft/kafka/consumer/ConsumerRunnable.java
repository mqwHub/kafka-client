package zx.soft.kafka.consumer;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import zx.soft.kafka.hbase.HbaseClient;
import zx.soft.kafka.utils.ConvertUtil;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author donglei
 *
 */
public class ConsumerRunnable implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

	private KafkaStream<byte[], byte[]> m_stream;

	private AtomicLong count;
	private MessageHandler handler;
	
	private ConcurrentHashMap<ByteWrapper, byte[]> _resultmap;
	
	private Object object;
	
	// 从低位到高位
	private static byte[] remarkOfEndStream = new byte[] {100, 101, 103, 104, 105};
	
	// PCAP文件标示
	private static int lenOfPlaceholder = 2;

	public ConsumerRunnable() {}
	
	public ConsumerRunnable(KafkaStream<byte[], byte[]> a_stream, AtomicLong count, MessageHandler handler, ConcurrentHashMap<ByteWrapper, byte[]> map, Object obj) {
		m_stream = a_stream;
		this.count = count;
		this.handler = handler;
		this._resultmap = map;
		this.object  = obj;
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
		byte[] receivedMsg = it.next().message();
		try {
			HbaseClient.put("kafka_db", convertToRowkey(receivedMsg), 
					"load", "content", convertToContent(receivedMsg));
		} catch (Exception e) {
			logger.error("write to hbase error : ", e);
		}
	}

	private byte[] convertToContent(byte[] receivedMsg) {
		byte[] load = new byte[receivedMsg.length - 24];
		System.arraycopy(receivedMsg, 24, load, 0, receivedMsg.length - 24);
		return load;
	}

	private String convertToRowkey(byte[] receivedMsg) {
		StringBuilder sb = new StringBuilder();
		byte[] buffer = new byte[2];
		byte[] order = new byte[4];
		for (int i = 0; i < 20; i++) {
			buffer[i % 2] = receivedMsg[i];
			if (i % 2 == 1) {
				sb.append(Hex.encodeHexString(buffer));
			}
		}
		// 文件的顺序位置标示
		System.arraycopy(receivedMsg, 20, order, 0, 4);
		sb.append('|').append(ConvertUtil.fromByteArray(order));
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