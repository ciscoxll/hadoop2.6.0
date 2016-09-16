package cn.com.csiic.reverseindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public   class ReverseIndexmMain {
	public  static class ReverseMapOne extends Mapper<LongWritable, Text, Text, LongWritable>{
		LongWritable v2 = new LongWritable(1);
		Text k2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\\s");
			FileSplit inputSplit =(FileSplit) context.getInputSplit();
			String filename = inputSplit.getPath().getName();
				for (String word : split) {
						k2.set(word+"-->"+filename);
					context.write(k2, v2);
				}
		
			
		}

	}
	
	public static class ReverseReduceone extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable v3 = new LongWritable();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> value,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum=0;
			for (LongWritable word : value) {
				sum+=word.get();
			}
			v3.set(sum);
			context.write(key, v3);
		}
		
	}
	public static class ReverseMaptwo extends Mapper<LongWritable, Text, Text, Text>{
		Text k2 = new Text();
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			String[] word = split[0].split("-->");
			k2.set(word[0]);
			v2.set(word[1]+"-->"+split[1]);
			context.write(k2, v2);
			
		}
	}
	public static class ReverseReducetwo extends Reducer<Text, Text, Text, Text>{
		Text v3 = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuffer strbuf = new StringBuffer();
			for (Text word : value) {
				strbuf.append(word.toString()).append(" ");
			}
			v3.set(strbuf.toString());
			context.write(key, v3);
			
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf01 = new Configuration();
		Job job01 = Job.getInstance(conf01);
		//job01.setJar("/home/ReverseIndexmMain.jar");
		job01.setJarByClass(ReverseIndexmMain.class);
		
		job01.setMapperClass(ReverseMapOne.class);
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(LongWritable.class);
		
		job01.setReducerClass(ReverseReduceone.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job01, new Path(args[0]));
		Path out01 = new Path(args[1]);
		out01.getFileSystem(conf01).delete(out01, true);
		FileOutputFormat.setOutputPath(job01, out01);
	/*	job01.setInputFormatClass(TextInputFormat.class);
		job01.setOutputFormatClass(TextOutputFormat.class);*/
		
		boolean ifjob_001 = job01.waitForCompletion(true);
		Configuration conf02 = new Configuration();
		Job job02 = Job.getInstance(conf02);
		job02.setJarByClass(ReverseIndexmMain.class);
		
		job02.setMapperClass(ReverseMaptwo.class);
		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(Text.class);
		
		job02.setReducerClass(ReverseReducetwo.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job02, out01);
		Path out02 = new Path(args[2]);
		out02.getFileSystem(conf02).delete(out02, true);
		FileOutputFormat.setOutputPath(job02, out02);
		if(ifjob_001){
			job02.waitForCompletion(true);
		}
	}

}
