package zx.soft.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.config.ConfigUtil;

/**
 * kafka消费者组
 * @author donglei
 *
 */
public class KafkaConsumerGroup {

	private static Logger logger = LoggerFactory.getLogger("KAFKA_LOG");
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	private AtomicLong count;
	private ConcurrentHashMap<ByteWrapper, byte[]> resultMap;
	private Object lock = new Object();

	public KafkaConsumerGroup(String a_topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic = a_topic;
		this.count = new AtomicLong();
		resultMap = new ConcurrentHashMap<ByteWrapper, byte[]>();
	}

	public void shutdown() {
		if (consumer != null) {
			consumer.shutdown();
		}
		if (executor != null) {
			executor.shutdown();
		}
	}

	/**
	 *
	 * @param a_numThreads
	 * @param consumerRunnable
	 */
	public void run(int a_numThreads, MessageHandler handler) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		executor = Executors.newFixedThreadPool(a_numThreads);

		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new ConsumerRunnable(stream, count, handler, resultMap, lock));
		}
		while (true) {
			logger.info("pull " + this.count.get() + " Records");
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
			}
		}
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties kafka = ConfigUtil.getProps("kafka.properties");
		logger.info("load properties :" + kafka.toString());
		Properties props = new Properties();
		props.put("zookeeper.connect", kafka.getProperty("zookeeper"));
		props.put("group.id", kafka.getProperty("group_id"));
		props.put("zookeeper.session.timeout.ms", "60000");
		props.put("zookeeper.sync.time.ms", "2000");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");
		return new ConsumerConfig(props);
	}

}