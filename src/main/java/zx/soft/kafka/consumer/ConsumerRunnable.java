package zx.soft.kafka.consumer;

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

	public ConsumerRunnable(KafkaStream<byte[], byte[]> a_stream, AtomicLong count, MessageHandler handler) {
		m_stream = a_stream;
		this.count = count;
		this.handler = handler;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		while (it.hasNext()) {
			this.handler.handleMessage(it.next().message());
			count.incrementAndGet();
		}
		logger.info("Shutting down Thread: " + this.hashCode());
	}

}