package cn.com.csiic.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.zookeeper.AsyncCallback.StatCallback;

public class InvertedIndexone {
	final static String INPUT="hdfs://master64:9000/data";
	final static String OUTPUT="hdfs://master64:9000/outdata";
	
	static class indexmap extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\\s");
			// 因为map方法是由maptask来调用的，而maptask知道自己所读的数据所在的切片
			// 进而，在切片信息中有切片所属的文件信息
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String filename = inputSplit.getPath().getName();
			
			for (String word : split) {
				context.write(new Text(word+"-->"+filename), new LongWritable(1));
			}
			
		}
	}
	static class indexreduce extends Reducer<Text,LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text key2, Iterable<LongWritable> value2,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum=0;
			for (LongWritable time : value2) {
				sum+=time.get();
			}
			context.write(key2, new LongWritable(sum));
			
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 设置打包jar
		job.setJarByClass(InvertedIndexone.class);
		job.setMapperClass(indexmap.class);
		job.setReducerClass(indexreduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(INPUT));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
		
		boolean waitForCompletion = job.waitForCompletion(true);
		
		
	}

}
