package cn.com.csiic.hadoop2_6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class mymaper extends Mapper<Text, LongWritable, Text, LongWritable>{
	@Override
	//static
	protected void map(Text key, LongWritable value,
			Mapper<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
	}

}
