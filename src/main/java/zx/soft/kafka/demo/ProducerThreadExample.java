package zx.soft.kafka.demo;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kafka.producer.ProducerInstance;

public class ProducerThreadExample {
	private static Logger logger = LoggerFactory.getLogger(ProducerThreadExample.class);

	public static void main(String args[]) throws InterruptedException, ExecutionException {
		int numOfThread = 10;
		ProducerInstance instance = ProducerInstance.getInstance();
		long start = System.currentTimeMillis();
		boolean sync = false;
		String topic = "test";

		ExecutorService executor = Executors.newFixedThreadPool(numOfThread);
		CountDownLatch latch = new CountDownLatch(numOfThread);
		int gap = 1000000;
		for (int i = 0; i < 10; i++) {
			executor.submit(new ProducerRunnable(instance, topic, sync, i * gap, gap, latch));
		}
		latch.await();
		executor.shutdown();
		instance.close();
		logger.info("Push " + (10 * gap) + " 　条数据,耗时为" + (System.currentTimeMillis() - start));
	}
}