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

import cn.com.csiic.SharedFriend.SharedFriendone.SharedFriendmapone;
import cn.com.csiic.SharedFriend.SharedFriendone.SharedFriendreduce;
/**
 * 共同好友
 * @author xiaoliangliang
 * A	I,K,C,B,G,F,H,O,D,
	B	A,F,J,E,
	C	A,E,B,H,F,G,K,
	D	G,C,K,A,L,F,E,H,
	E	G,M,L,H,A,F,B,D,
	F	L,M,D,C,G,A,
	G	M,
	H	O,
	I	O,C,
	J	O,
	K	B,
	L	D,E,
	M	E,F,
	O	A,H,I,J,F,
 */
public class SharedFriendTwo extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SharedFriendTwo(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFriendTwo.class);
		
		job.setMapperClass(SharedFriendtwomap.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(SharedFriendtworeduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		Path output = new Path(args[1]);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
		return 0;
	}
	static class SharedFriendtwomap extends Mapper<LongWritable , Text , Text, Text>{
		Text k2 = new Text();
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] pANDf = value.toString().split("\t");
			String[] friend = pANDf[1].split(",");
			for(int i=0;i<friend.length-1;i++){
				for (int j = 1; j < friend.length-2; j++) {
					k2.set(friend[i]+"-->"+friend[j]);
					v2.set(pANDf[0]);
					context.write(k2, v2);
				}
			}
		}
		
	}
	static class SharedFriendtworeduce extends Reducer<Text, Text, Text, Text>{
		Text v3 = new Text();
		@Override
		protected void reduce(Text k2, Iterable<Text> v2,Context context)
				throws IOException, InterruptedException {
			StringBuffer strbuf = new StringBuffer();
			for (Text friend : v2) {
				strbuf.append(friend.toString()).append(" ");
			}
			v3.set(strbuf.toString());
			context.write(k2, v3);
		}
	}
	
}
