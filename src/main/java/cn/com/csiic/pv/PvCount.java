package cn.com.csiic.pv;




import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.junit.Test;

import sun.tools.tree.NewArrayExpression;

public class PvCount {
	public static String getTimeDiff(long time1, long time2) {
		long l = time2 - time1;
		long day = l / (24 * 60 * 60 * 1000);
		long hour = (l / (60 * 60 * 1000) - day * 24);
		long min = ((l / (60 * 1000)) - day * 24 * 60 - hour * 60);
		long s = (l / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
		return "" + day + "天" + hour + "小时" + min + "分" + s + "秒";
	}

	
	@Test
	public void fun() throws Exception {
		String str="27.19.74.143 - - [30/May/2013:17:38:25";
		String[] split = str.split(" - - \\[");
		System.out.println(split[1]);
		/*SimpleDateFormat sd2 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
		SimpleDateFormat sd3 = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
		String format = sd3.format(new Date(split[1]));
		System.out.println(format);*/
		//Date date = new Date();
		System.out.println(new Date());
		SimpleDateFormat sd3 = new SimpleDateFormat("yyyy-MM-dd  HH:mm:ss");
		String format = sd3.format(new Date());
		System.out.println(format);
		
	
	}//

}
