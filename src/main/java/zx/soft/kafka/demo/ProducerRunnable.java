package zx.soft.kafka.demo;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kafka.producer.ProducerInstance;

public class ProducerRunnable implements Runnable {
	private Logger logger = LoggerFactory.getLogger(ProducerExample.class);

	private ProducerInstance producer;
	private int base;
	private int gap;
	private String topic;
	private boolean sync;
	private CountDownLatch latch;

	public ProducerRunnable(ProducerInstance producer, String topic, boolean sync, int base, int gap,
			CountDownLatch latch) {
		this.producer = producer;
		this.topic = topic;
		this.base = base;
		this.gap = gap;
		this.sync = sync;
		this.latch = latch;
	}

	@Override
	public void run() {
		try {
			for (int i = 0; i < gap; i++) {
				if (i % 1000 == 0) {
					logger.info("i = " + i);
				}
				this.producer.pushRecord(topic, (base + i) + "");
			}
		} finally {
			this.latch.countDown();
		}
	}

}
