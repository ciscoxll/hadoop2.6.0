package cn.com.csiic.diywordcount;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DiyWordCount extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=2||args==null){
			System.err.println("参数必须有3个，分别是[inputPath]   [outputPath]    ");
		}
		Configuration conf =getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DiyWordCount.class);
		
		job.setMapperClass(diymapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(diyreducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		String input=args[0];
		Path output = new Path(args[1]);
		// 接受参数
		//conf.set("keywords",args[2]);
		
		FileInputFormat.setInputPaths(job, input);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
		
		return 0;
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DiyWordCount(), args);
	}
	
	public static class diymapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		HashSet<String> hashSet = new HashSet<String>();
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String keywords = conf.get("keywords");
			try {
				String[] word = keywords.split(",");
				for (String string : word) {
					hashSet.add(string);
				}
				
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			
		}
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
			for (String word : split) {
				if(hashSet.contains(word)){
					k2.set(word);
					v2.set(1);
					context.write(k2, v2);
				}
			}
		}
	}
	public static class diyreducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable v3 = new LongWritable();
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum=0;
			for (LongWritable times : v2) {
				sum+=times.get();
			}
			v3.set(sum);
			context.write(k2, v3);
		}
	}
	
}
