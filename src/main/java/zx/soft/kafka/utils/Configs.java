package zx.soft.kafka.utils;

import java.io.InputStream;
import java.util.Properties;

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

}
