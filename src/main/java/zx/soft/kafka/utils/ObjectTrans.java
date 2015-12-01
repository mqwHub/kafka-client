package zx.soft.kafka.utils;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;

public class ObjectTrans {

	public static final DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

	//将hbase　Get结果Result转换为T类的对象
	public static <T> T Result2Object(Result result, Class<T> cls) throws InstantiationException,
			IllegalAccessException, NoSuchFieldException, SecurityException {

		List<Cell> cells = result.listCells();
		T object = cls.newInstance();
		String qualifier = null;
		String value;
		if (cells != null) {
			for (Cell cell : cells) {
				qualifier = new String(CellUtil.cloneQualifier(cell));
				value = new String(CellUtil.cloneValue(cell));
				Field field = object.getClass().getDeclaredField(qualifier);
				field.setAccessible(true);
				if (field.getType() == Integer.class || field.getType() == int.class) {
					field.set(object, Integer.valueOf(value));
				} else if (field.getType() == Long.class || field.getType() == long.class) {
					field.set(object, Long.valueOf(value));
				} else if (field.getType() == boolean.class) {
					field.setBoolean(object, Boolean.getBoolean(value));
				} else if (field.getType() == Date.class) {
					Date date = null;
					try {
						date = dateFormat.parse(value);
					} catch (ParseException e) {
						date = new Date(Long.valueOf(value));
					}
					field.set(object, date);
				} else {
					field.set(object, value);
				}
			}
		}
		return object;
	}

	//通过查询获得的Result获取指定列族，标志符的值
	public static String getValue(String cf, String qualifier, Result result) {
		String value = null;
		if (result.containsNonEmptyColumn(cf.getBytes(), qualifier.getBytes())) {
			Cell cell = result.getColumnLatestCell(cf.getBytes(), qualifier.getBytes());
			value = new String(CellUtil.cloneValue(cell));
		}
		return value;
	}

}
