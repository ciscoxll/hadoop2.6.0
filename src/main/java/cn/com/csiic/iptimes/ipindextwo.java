package cn.com.csiic.iptimes;

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

public class ipindextwo {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ipindextwo.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ipmapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(ipreduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.waitForCompletion(true);
	}
	
	public static class ipmapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k2 = new Text();
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("-->");
			
			context.write(new Text(split[0]),new Text(split[1]+" "));
			
		}
		
	}
	public static class ipreduce extends Reducer<Text, Text, Text, Text>{
		@Override
			protected void reduce(Text K3, Iterable<Text> V3,
					Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException{
				StringBuffer strbuf = new StringBuffer();
				int times=0;
				for (Text text : V3) {
					strbuf.append(text);
					times++;
				}
				if(times!=1)
				{
				context.write(K3, new Text(strbuf.toString()));
				}
			
				
		}
	}

}
