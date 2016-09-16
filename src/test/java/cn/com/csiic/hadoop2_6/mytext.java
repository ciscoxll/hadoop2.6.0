package cn.com.csiic.hadoop2_6;

import static org.junit.Assert.*;

import org.junit.Test;

public class mytext {
	@Test
	public void testName() throws Exception {
		String a="13429100031 22554 8 2013-03-11 08:55:19.151754088 571 571 282 571";
		String[] split = a.split("\\s+");
		for (String string : split) {
			System.out.println(string);
		}
	}
}
