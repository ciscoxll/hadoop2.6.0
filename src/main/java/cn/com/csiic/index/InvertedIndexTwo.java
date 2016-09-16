package cn.com.csiic.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexTwo {
	final static String INPUT="hdfs://master64:9000/outdata";
	final static String OUTPUT="hdfs://master64:9000/outdataTwo";
	static class indexmap extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		/*hello-->a.txt	3
		hello-->b.txt	2
		hello-->c.txt	2
		jerry-->a.txt	1
		jerry-->b.txt	3
		jerry-->c.txt	1
		tom-->a.txt	2
		tom-->b.txt	1
		tom-->c.txt	1*/

		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\\s");
			String[] word = split[0].split("-->");
			String wordname=word[0];
			String filename = word[1];
			context.write(new Text(wordname), new Text(filename+"-->"+split[1]));
			
		}
		
	}
	static class indexreduce extends Reducer<Text,Text, Text, Text>{
		// 输入：<hello,a.txt-->3><hello,b.txt-->2><hello,c.txt-->1>
		// 输出： hello a.txt-->3 b.txt-->2 c.txt -->2
		@Override
		protected void reduce(Text key3, Iterable<Text> value3,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer str = new StringBuffer();
			for (Text buf : value3) {
				str.append(buf+" ");
			}
			context.write(key3, new Text(str.toString()));
			
			
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 设置打包jar
		job.setJarByClass(InvertedIndexTwo.class);
		
		job.setMapperClass(indexmap.class);
		job.setReducerClass(indexreduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(INPUT));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
		
		job.waitForCompletion(true);
		
	}

}
