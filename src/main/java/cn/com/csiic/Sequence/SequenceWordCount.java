package cn.com.csiic.Sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class SequenceWordCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SequenceWordCount.class);
		
		job.setMapperClass(squence.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(sequenceReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path output = new Path(args[1]);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job,output);
		job.waitForCompletion(true);
	}

	public static class squence extends Mapper<Text, Text, Text, LongWritable>{
		LongWritable v2 = new LongWritable(1);
		Text k2 = new Text();
		@Override
		protected void map(Text key, Text value,
				Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] ling = value.toString().split("\n");
				for (String string : ling) {
					String[] word = string.split(" ");
						for (String string2 : word) {
							k2.set(key.toString()+"-->"+string2);
							context.write(k2, v2);
						}
				}
		}
		
	}
	public static class sequenceReduce extends Reducer<Text, LongWritable, Text,LongWritable>{
		LongWritable v3 = new LongWritable();
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum=0;
			for (LongWritable time : v2) {
				sum+=time.get();
			}
			v3.set(sum);
			context.write(k2, v3);
			
		}
		
	}
}
