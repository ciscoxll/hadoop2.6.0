package cn.com.csiic.sort;

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

public class NumberSort {
	public static void main(String[] args) throws Exception {
		String input = args[0];
		Path output = new Path(args[1]);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(NumberSort.class);
		
		job.setMapperClass(nummap.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(numreducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, input);
		//删除输出目录
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job,output);
		job.waitForCompletion(true);
		
	}
	static long sum=1;
	public static class nummap extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
		LongWritable number = new LongWritable();
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String sttnum = value.toString().trim();
			if (sttnum.length()!=0) {
				long num = Long.parseLong(sttnum);
				number.set(num);
				context.write(number, new LongWritable(1));
			}
			
		}
	}
	/*public  static class numcom extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
		
	}*/
	public static class numreducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
		@Override
		protected void reduce(
				LongWritable k3,
				Iterable<LongWritable> v3,
				Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context)
				throws IOException, InterruptedException {
			for (LongWritable num : v3) {
				
				context.write(new LongWritable(sum), k3);
				sum+=num.get();
			}
			
		}
	}

}
