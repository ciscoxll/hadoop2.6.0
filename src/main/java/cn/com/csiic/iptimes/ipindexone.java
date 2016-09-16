package cn.com.csiic.iptimes;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class ipindexone {
	final static String input="hdfs://master64:9000/ip/input";
	final static String output="hdfs://master64:9000/ip/out";
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ipindexone.class);
		
		job.setMapperClass(ipmap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(ipreduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		
	}
		public static class ipmap extends Mapper<LongWritable, Text, Text, LongWritable>{
			
			private  Text text = new Text();;
			/**
			 * 192.168.1.1		192.168.1.13
			 * 192.168.2.1		192.168.1.13
			 * 192.168.1.1		192.168.1.13
			 * 192.168.1.1		192.168.1.13
			 * */
			@Override
			protected void map(
					LongWritable key,
					Text value,
					Mapper<LongWritable, Text, Text, LongWritable>.Context context)
					throws IOException, InterruptedException {
				String[] split = value.toString().split("\\s");
				FileSplit inputSplit = (FileSplit)context.getInputSplit();
				String filename = inputSplit.getPath().getName();
				
				for (String ip : split) {
					context.write(new Text(ip+"-->"+filename), new LongWritable(1));
				}
			}
		}
		public static class ipreduce extends Reducer<Text, LongWritable, Text, NullWritable>{
			@Override
			protected void reduce(Text k3, Iterable<LongWritable> arg1,
					Reducer<Text, LongWritable, Text, NullWritable>.Context context)
					throws IOException, InterruptedException {
				context.write(k3, null);
			}
		}
}
