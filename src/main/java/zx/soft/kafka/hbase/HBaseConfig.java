package zx.soft.kafka.hbase;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import zx.soft.utils.config.ConfigUtil;

public class HBaseConfig {

	public static Configuration getZookeeperConf() {
		Properties prop = ConfigUtil.getProps("zookeeper.properties");
		//在classpath下查找hbase-site.xml文件，如果不存在，则使用默认的hbase-core.xml文件
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"));
		return conf;
	}
}
