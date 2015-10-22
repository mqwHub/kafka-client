package zx.soft.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kafka.utils.Configs;

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
		int batchSize = Integer.parseInt(Configs.getProps("batch_size"));
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		List<byte[]> messages = new ArrayList<>();
		while (it.hasNext()) {
			messages.add(it.next().message());
			if (messages.size() >= batchSize) {
				this.handler.handleMessage(messages);
				messages.clear();
			}
			count.incrementAndGet();
		}
		if (!messages.isEmpty()) {
			this.handler.handleMessage(messages);
			messages.clear();
		}
		logger.info("Shutting down Thread: " + this.hashCode());
	}
}