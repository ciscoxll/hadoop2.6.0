package cn.com.csiic.cache;

import java.io.File;
import java.io.IOException;


import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * **
 * 在map函数中读取外部加载的文件
 *
 *
 *
 * @author xiaoliangliang
 */
public class CacheFileApp {
	public static void main(String[] args) throws Exception {
		String input=args[0];
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CacheFileApp.class);
		job.setMapperClass(cachemap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(cacheducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//使用刚才上传到hdfs的外部依赖文件，这里一定要使用别名
		job.addCacheFile(new URI("/ips#ips"));
		FileInputFormat.setInputPaths(job, input);
		Path output = new Path(args[1]);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
	}
	static class cachemap extends Mapper<LongWritable, Text, Text,LongWritable>{
		String readFileToString;
		Text k2 = new Text();
		LongWritable v2 = new LongWritable(1);
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			readFileToString = FileUtils.readFileToString(new File("./ips"));
		}
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			for (String string : split) {
				k2.set(string);
				//v2.set(1);
				context.write(k2,v2 );
				System.out.println("调用setup内容"+readFileToString);
			}
		}
		
		
		
	}
	static class cacheducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable v3 = new LongWritable();
		@Override
		protected void setup(
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String readFileToString = FileUtils.readFileToString(new File("./ips"));
			context.write(new Text("wo ha ha "+readFileToString), new LongWritable());
		}
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum=0;
			for (LongWritable word : v2) {
				sum+=word.get();
			}
			v3.set(sum);
			context.write(k2, v3);
		}
		
	}

}
