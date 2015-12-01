package zx.soft.kafka.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.protobuf.ServiceException;

public class HBaseClient {

	private static HBaseAdmin hbaseAdmin;

	public HBaseClient() throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
		hbaseAdmin = new HBaseAdmin(HBaseConfig.getZookeeperConf());
		HBaseAdmin.checkHBaseAvailable(HBaseConfig.getZookeeperConf());
	}

	/**
	 * 创建表，默认版本数量是无限
	 * @param tableName　表名
	 * @param columnFamilys　列族
	 * @throws IOException
	 */
	public boolean createTable(String tableName, String... columnFamilys) throws IOException {
		boolean success = false;
		if (!(hbaseAdmin.tableExists(tableName))) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			for (String columnFamily : columnFamilys) {
				tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			}
			try {
				hbaseAdmin.createTable(tableDescriptor);
				success = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return success;
	}

	public boolean createTable(String tableName) throws IOException {
		boolean success = false;
		if (!(hbaseAdmin.tableExists(tableName))) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			try {
				hbaseAdmin.createTable(tableDescriptor);
				success = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return success;
	}

	public void addFamily(String tableName, String... columnFamilys) throws IOException {
		for (String columnFamily : columnFamilys) {
			hbaseAdmin.addColumn(tableName, new HColumnDescriptor(columnFamily));
		}
	}

	public void deleteFamily(String tableName, String... columnFamilys) throws IOException {
		for (String columnFamily : columnFamilys) {
			hbaseAdmin.deleteColumn(tableName, columnFamily);
		}
	}

	/**
	 * 创建表
	 * @param tableName　表名
	 * @param maxVersion　　版本数
	 * @param columnFamilys　列族
	 * @return
	 */
	public boolean createTable(String tableName, int maxVersion, String... columnFamilys) {
		boolean success = false;
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
		for (String columnFamily : columnFamilys) {
			HColumnDescriptor coldef = new HColumnDescriptor(columnFamily);
			coldef.setMaxVersions(maxVersion);
			desc.addFamily(coldef);
		}
		try {
			hbaseAdmin.createTable(desc);
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return success;
	}

	/**
	 * 判断指定表名的表是否存在
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public boolean isTableExists(String tableName) throws IOException {
		return hbaseAdmin.tableExists(tableName);
	}

	/**
	 * 删除指定表名的表,若待删除的表不存在，返回false
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public boolean deleteTable(String tableName) throws IOException {
		boolean deleted = false;
		if (hbaseAdmin.tableExists(tableName)) {
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
			deleted = true;
		}
		return deleted;
	}

	/**
	 * 关闭hbaseAdmin
	 * @throws IOException
	 */
	public void close() throws IOException {
		hbaseAdmin.close();
	}

	public static void main(String[] args) throws Exception {
		HBaseClient client = new HBaseClient();
		client.addFamily("kafka_db", "load");
		
	}
}
