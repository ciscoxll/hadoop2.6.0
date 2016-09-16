package cn.com.csiic.SharedFriend;

import java.io.IOException;

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


/**
 * 		求共同好友
 * @author xiaoliangliang
 * A:B,C,D,F,E,O
	B:A,C,E,K
	C:F,A,D,I
	D:A,E,F,L
 *
 */

public class SharedFriendone extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SharedFriendone(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf =getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFriendone.class);
		
		job.setMapperClass(SharedFriendmapone.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(SharedFriendreduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		Path output = new Path(args[1]);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
		
		return 0;
	}
	static class SharedFriendmapone extends Mapper<LongWritable, Text, Text, Text>{
		Text k2 = new Text();
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] persionAndFriend = value.toString().split(":");
			String[] friends = persionAndFriend[1].split(",");
			for (String friend : friends) {
				k2.set(friend);
				v2.set(persionAndFriend[0]);
				context.write(k2, v2);
			}
		}
		
	}
	static class SharedFriendreduce extends Reducer<Text, Text, Text, Text>{
		Text v3 = new Text();
		@Override
		protected void reduce(Text k2, Iterable<Text> v2,Context context)
				throws IOException, InterruptedException {
			StringBuffer strbuf = new StringBuffer();
			for (Text text : v2) {
				strbuf.append(text).append(",");
			}
			v3.set(strbuf.toString());
			context.write(k2, v3);
		}
	}
	
}
