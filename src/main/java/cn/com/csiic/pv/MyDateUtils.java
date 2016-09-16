package cn.com.csiic.pv;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class MyDateUtils {
	public static String dateFormat2String = "yyyy-MM-dd HH:mm:ss";

	public static SimpleDateFormat dateFormat1 = new SimpleDateFormat(
			"yyyy年MM月dd日 HH时mm分 E");
	public static SimpleDateFormat dateFormat2 = new SimpleDateFormat(
			MyDateUtils.dateFormat2String);
	public static SimpleDateFormat dateFormat3 = new SimpleDateFormat(
			"yyyy年MM月dd日  E");
	public static SimpleDateFormat dateFormat4 = new SimpleDateFormat(
			"yyyy-MM-dd");
	public static SimpleDateFormat dateFormat5 = new SimpleDateFormat(
			"MM月dd日HH时 E");
	public static SimpleDateFormat dateFormat6 = new SimpleDateFormat("yyyy-MM");
	public static SimpleDateFormat dateFormat7 = new SimpleDateFormat(
			"yyyy年MM月dd日 HH时mm分ss秒");
	public static SimpleDateFormat dateFormat8 = new SimpleDateFormat(
			"dd日MM月yyyy年 HH时mm分ss秒");

	/**
	 * 格式：yyyy年MM月dd日 HH时mm分 E
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate1(Date date) {
		return dateFormat1.format(date);
	}

	/**
	 * 格式：yyyy-MM-dd HH:mm:ss
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate2(Date date) {
		return dateFormat2.format(date);
	}

	/**
	 * 格式：yyyy年MM月dd日 E
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate3(Date date) {
		return dateFormat3.format(date);
	}

	/**
	 * 格式：yyyy-MM-dd
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate4(Date date) {
		return dateFormat4.format(date);
	}

	/**
	 * 格式：MM月dd日HH时 E
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate5(Date date) {
		return dateFormat5.format(date);
	}

	/**
	 * 格式：yyyy-MM
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate6(Date date) {
		return dateFormat6.format(date);
	}

	/**
	 * 格式：yyyy年MM月dd日HH时mm分ss秒
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDate7(Date date) {
		return dateFormat7.format(date);
	}
	public static String formatDate8(Date date) {
		return dateFormat8.format(date);
	}

	public static Date today() {
		Calendar cal = new GregorianCalendar();// 默认是当前时间(long)
		return cal.getTime();
	}

	public static Date yesterday() {
		Calendar cal = new GregorianCalendar();// 默认是当前时间(long)
		cal.add(Calendar.DAY_OF_MONTH, -1);
		return cal.getTime();
	}

	public static Date monthBeginDate() {
		Calendar cal = new GregorianCalendar();// 默认是当前时间(long)
		cal.set(Calendar.DAY_OF_MONTH, 1);
		return cal.getTime();
	}

	public static Date monthEndDate() {
		Calendar cal = new GregorianCalendar();// 默认是当前时间(long)
		cal.set(Calendar.DAY_OF_MONTH, 0);
		return cal.getTime();
	}

	/**
	 * 自定义日前类型
	 * 
	 * @param formatString
	 *            格式
	 * @param dateString
	 *            日期值
	 * @return
	 */
	public static Date format(String formatString, String dateString) {
		try {
			return (new SimpleDateFormat(formatString)).parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 格式化类型yyyy-MM-dd
	 * 
	 * @param dateString
	 * @return
	 */
	public static Date format(String dateString) {
		try {
			return dateFormat4.parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 获取两个时间的差，返回形式：XX天XX小时XX分XX秒
	 * 
	 * @param time1
	 * @param time2
	 * @return
	 */
	public static String getTimeDiff(long time1, long time2) {
		long l = time2 - time1;
		long day = l / (24 * 60 * 60 * 1000);
		long hour = (l / (60 * 60 * 1000) - day * 24);
		long min = ((l / (60 * 1000)) - day * 24 * 60 - hour * 60);
		long s = (l / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
		return "" + day + "天" + hour + "小时" + min + "分" + s + "秒";
	}

	public static void main(String[] args) {
		Calendar cal = new GregorianCalendar();// 默认是当前时间(long)
		cal.add(Calendar.MINUTE, 30);
		System.out.println(MyDateUtils.formatDate7(cal.getTime()));
	}
}
