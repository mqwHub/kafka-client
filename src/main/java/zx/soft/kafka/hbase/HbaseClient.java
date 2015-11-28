package zx.soft.kafka.hbase;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseClient {

	static Configuration cfg = HBaseConfiguration.create();

	public static void create(String tablename, String columnFamily) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if (admin.tableExists(tablename)) {
			System.out.println("table exists!");
			System.exit(0);
		} else {
			HTableDescriptor desc = new HTableDescriptor(tablename);
			desc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(desc);
			System.out.println("create table success!");
		}

	}

	public static boolean delete(String tableName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}

		}
		return true;
	}

	public static void put(String tableName, String row, String columnFamily, String column, String data)
			throws Exception {
		HTable table = new HTable(cfg, tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
		table.put(p1);
	}

	public static void put(String tableName, String row, String columnFamily, String column, byte[] data)
			throws Exception {
		HTable table = new HTable(cfg, tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), data);
		table.put(p1);
	}

	public static void main(String[] args) throws Exception {
		
		byte[] b = new byte[1024];
		FileInputStream in = new FileInputStream("src/main/resources/kafka.properties");
        BufferedInputStream bin = new BufferedInputStream(in);
        DataInputStream din = new DataInputStream(bin);
        din.read(b);
		String tablename = "testdb";
		String columnFamily = "cf";
		try {
			HbaseClient.delete(tablename);
			HbaseClient.create(tablename, columnFamily);
			HbaseClient.put(tablename, "row1", columnFamily, "cl1", b);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
