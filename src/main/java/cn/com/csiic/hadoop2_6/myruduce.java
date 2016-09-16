package cn.com.csiic.hadoop2_6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myruduce extends Reducer<Text, LongWritable, Text, LongWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2)
			throws IOException, InterruptedException {
		
	}

}
