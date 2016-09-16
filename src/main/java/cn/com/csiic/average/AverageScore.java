package cn.com.csiic.average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageScore {
	public static void main(String[] args) throws Exception {
		String inpit = args[0];
		Path output = new Path(args[1]);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(AverageScore.class);
		
		job.setMapperClass(averagemap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(averagereduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, inpit);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
	}
	public static class averagemap extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(" ");
			String name = split[0];
			k2.set(name);
			String score = split[1].trim();
			if(score.length()!=0){
				long longscore = Long.parseLong(score);
				v2.set(longscore);
				context.write(k2, v2);
			}
			
		}
	}
	public static class averagereduce extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable v3 = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> value,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum=0;
			long temp=0;
			for(LongWritable score:value){
				sum+=score.get();
				temp++;
			}
		v3.set(sum/temp);
		context.write(key, v3);
			
		}
	}
}
