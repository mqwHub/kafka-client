package zx.soft.kafka.consumer;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

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
			synchronized(object) {
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
		}
		logger.info("Shutting down Thread: " + this.hashCode());
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
		byte[] receivedMsg = new byte[] {0x12, 0x13, 0x14, 0x15, 0x16, 0x17};
		byte[] result = r.getFileRemarkkey(receivedMsg, 3);
		byte[] r1 = r.getCucurrentValue(receivedMsg, 4);
		System.out.println(result.length);
		for (byte b : result) {
			System.out.println(b);
		}
		System.out.println(r1.length);
		for (byte b : r1) {
			System.out.println(b);
		}
	}
}