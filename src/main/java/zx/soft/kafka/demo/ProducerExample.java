package zx.soft.kafka.demo;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kafka.producer.ProducerInstance;
import zx.soft.utils.checksum.CheckSumUtils;

public class ProducerExample {
	private static Logger logger = LoggerFactory.getLogger(ProducerExample.class);

	public static void main(String args[]) throws InterruptedException, ExecutionException {
		ProducerInstance instance = ProducerInstance.getInstance();
		long start = System.currentTimeMillis();
		boolean sync = false;
		String topic = "test";
		int i = 10;
		for (; i < 20; i++) {
			if (i % 100 == 0) {
				logger.info("i = " + i);
			}
			instance.pushRecord(topic, CheckSumUtils.getMD5(i + ""));
		}
		instance.close();
		logger.info("Push " + i + " 　条数据,耗时为" + (System.currentTimeMillis() - start));
	}
}