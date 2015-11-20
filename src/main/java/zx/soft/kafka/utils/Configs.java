package zx.soft.kafka.utils;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.log.LogbackUtil;

/**
 *@author donglei
 */
public class Configs {
	private static Logger logger = LoggerFactory.getLogger(Configs.class);
	private static Properties props = new Properties();
	static {
		logger.info("Load resource: kafka.properties");
		try (InputStream in = ConfigUtil.class.getClassLoader().getResourceAsStream("kafka.properties");) {
			props.load(in);
			logger.info("kafka properties : " + props.toString());
		} catch (Exception e) {
			logger.error("Exception:{}", LogbackUtil.expection2Str(e));
			throw new RuntimeException(e);
		}

	}

	public static String getProps(String prop) {
		return props.getProperty(prop);
	}
	
	public static void  main(String[] args) {
		byte[] data1 ={0x12,0x13,0x14}; 
		byte[] data2 ={0x15,0x16,0x17};
		byte[] result = new byte[data1.length + data2.length];
		System.arraycopy(data1, 0, result, 0, data1.length);
		System.arraycopy(data2, 0, result, data1.length, data2.length);
		for (byte b : result) {
			System.out.println(b);
		}
	}
}
