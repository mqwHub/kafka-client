package zx.soft.kafka.hbase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kafka.demo.ConsumerGroupExample;
import zx.soft.kafka.utils.ObjectTrans;

public class HBaseTable {

	private HTableInterface table;
	private List<Put> puts;
	
	private Logger logger = LoggerFactory.getLogger("KAFKA_LOG");

	public HBaseTable(HConnection conn, String tableName) throws IOException {
		table = conn.getTable(tableName);
		table.setAutoFlushTo(true);//设置自动flush否则数据无法实时观察到
		puts = new ArrayList<>();
	}

	/**
	 * 添加或修改一行的值
	 */
	public void put(String rowKey, String family, String qualifer, String value) throws IOException {
		Put put = new Put(rowKey.getBytes());
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifer), Bytes.toBytes(value));
		puts.add(put);
	}

	public void put(String rowKey, String family, String qualifer, int value) throws IOException {
		Put put = new Put(rowKey.getBytes());
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifer), Bytes.toBytes(value));
		puts.add(put);
	}

	public void put(String rowKey, String family, String qualifer, long value) throws IOException {
		Put put = new Put(rowKey.getBytes());
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifer), Bytes.toBytes(value));
		puts.add(put);
	}

	public void put(String rowKey, String family, String qualifer, boolean value) throws IOException {
		Put put = new Put(rowKey.getBytes());
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifer), Bytes.toBytes(value));
		puts.add(put);
	}

	public void put(String rowKey, String family, String qualifer, byte[] value) throws IOException {
		Put put = new Put(rowKey.getBytes());
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifer), value);
		puts.add(put);
		logger.info("put '" + rowKey + "','" + family + ":" + qualifer + "','data'");
	}
	/**
	 * 将指定的列和对应的值及时间戳添加到Put实例中
	 */
	public void put(String rowKey, String family, String qualifer, long ts, String value) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifer), ts, Bytes.toBytes(value));
		puts.add(put);
	}

	/**
	 * 插入一个对象到数据表中，对应
	 */
	public <T> void putObject(String rowKey, String family, T t) throws InstantiationException, IllegalAccessException,
			IOException {
		Field[] fields = t.getClass().getDeclaredFields();
		Put put = new Put(Bytes.toBytes(rowKey));
		for (Field field : fields) {
			field.setAccessible(true);
			//修改bug，只有获取的对象不为空时写入hbase
			if (field.get(t) != null) {
				put.add(Bytes.toBytes(family), Bytes.toBytes(field.getName()),
						field.get(t).toString().getBytes(Charset.forName("UTF-8")));
			}
		}
		puts.add(put);
	}

	/**
	 * 插入多行到数据表中
	 */
	public <T> void putObjects(List<String> rowKeys, String family, List<T> ts) throws InstantiationException,
			IllegalAccessException, IOException {
		if (rowKeys.size() == ts.size()) {
			for (int i = 0; i < rowKeys.size(); i++) {
				this.putObject(rowKeys.get(i), family, ts.get(i));
			}
		}
	}

	/**
	 * 获得指定rowKey的对象
	 */
	public <T> T getObject(String rowKey, Class<T> cls) throws IOException, InstantiationException,
			IllegalAccessException, NoSuchFieldException, SecurityException, ParseException {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		T t = ObjectTrans.Result2Object(result, cls);
		return t;
	}

	/**
	 * 获取指定rowKey，列族，标识符的值value，一般用于查询对象的某个字段的值
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @return
	 * @throws IOException
	 */
	public Result get(String rowKey, String family, String qualifier) throws IOException {
		Get get = new Get(rowKey.getBytes());
		Result result = table.get(get);
		return result;
	}

	/**
	 *查询指定行键
	 */
	public Result get(String rowKey) throws IOException {
		Get get = new Get(rowKey.getBytes());
		Result result = table.get(get);
		return result;
	}

	/**
	 * 删除指定rowKey的行
	 */
	public void delete(String rowKey) throws IOException {
		Delete delete = new Delete(rowKey.getBytes());
		table.delete(delete);
	}

	/**
	 * 返回行键比较的结果
	 * @param rowKey
	 * @param compareOp -2代表小于，-1代表小于等于，０代表等于，１代表大于等于，２代表大于
	 */
	public List<Result> scan(String rowKey, int compareOp) throws IOException {
		List<Result> list = new ArrayList<>();
		Filter filter = null;
		switch (compareOp) {
		case -2:
			filter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(rowKey)));
			break;
		case -1:
			filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
		case 0:
			filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
		case 1:
			filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(rowKey)));
		default:
			filter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(rowKey)));
		}

		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		try {
			for (Result result : scanner) {
				list.add(result);
			}
		} finally {
			scanner.close();
		}
		return list;
	}

	/**
	 * 扫描在一段时间戳范围内的hbase行
	 * @param minStamp　毫秒数值
	 * @param maxStamp　毫秒数值
	 */
	public List<Result> scan(long minStamp, long maxStamp) throws IOException {
		List<Result> results = new ArrayList<>();
		Scan scan = new Scan();
		scan.setTimeRange(minStamp, maxStamp);
		ResultScanner scanner = table.getScanner(scan);
		try {
			for (Result result : scanner) {
				results.add(result);
			}
		} finally {
			scanner.close();
		}
		return results;
	}

	/**
	 * 执行put操作
	 */
	public void execute() {
		try {
			table.put(puts);
			puts = new ArrayList<Put>();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 根据插入表的时间统计数据量，添加列族和标志符加快统计
	 */
	public long getRowsCount(long minStamp, long maxStamp, String cf, String q) throws IOException {
		long count = 0;
		Scan scan = new Scan();
		scan.setTimeRange(minStamp, maxStamp);
		scan.addColumn(cf.getBytes(), q.getBytes());
		ResultScanner scanner = table.getScanner(scan);
		try {
			for (Result r : scanner) {
				count++;
			}
		} finally {
			scanner.close();
		}
		return count;
	}

	/**
	 * 关闭表和连接
	 */
	public void close() {
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
