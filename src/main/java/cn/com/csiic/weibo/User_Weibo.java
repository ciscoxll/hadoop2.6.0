package cn.com.csiic.weibo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;



public class User_Weibo {
	public static void main(String[] args) throws Exception {
		runjob01(args);
		runjob002(args);
		
	}
	public static void runjob002(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		String inpit = args[1];
		Path output = new Path(args[2]);
		Configuration conf = new Configuration();
		Job job002 = Job.getInstance(conf);
		job002.setJarByClass(User_Weibo.class);
		job002.setMapperClass(weiboMapTwo.class);
		job002.setMapOutputKeyClass(myLongWritable.class);
		job002.setMapOutputValueClass(Text.class);
		
		job002.setReducerClass(weiboReducerTwo.class);
		job002.setOutputKeyClass(myLongWritable.class);
		job002.setOutputValueClass(Text.class);
		//job002.setCombinerClass(myLongWritable.class);
		
		FileInputFormat.setInputPaths(job002, inpit);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job002, output);
		
		job002.waitForCompletion(true);
	}
	public static void runjob01(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		String inpit = args[0];
		Path output = new Path(args[1]);
		Configuration conf = new Configuration();
		Job job001 = Job.getInstance(conf);
		job001.setJarByClass(User_Weibo.class);
		job001.setMapperClass(weiboMapOne.class);
		job001.setMapOutputKeyClass(Text.class);
		job001.setMapOutputValueClass(Text.class);
		//job001.setNumReduceTasks(0);
		job001.setReducerClass(weiboReduceOen.class);
		job001.setOutputKeyClass(Text.class);
		job001.setOutputValueClass(LongWritable.class);
		job001.addArchiveToClassPath(new Path("/json-20140107.jar"));
		
		FileInputFormat.setInputPaths(job001, inpit);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job001, output);
		
		job001.waitForCompletion(true);
	}
	public static class weiboMapOne extends Mapper<LongWritable, Text, Text, Text>{
		Text k2 = new Text();
		Text v2 = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String file_name = inputSplit.getPath().getName();
			//String file_name = inputSplit.getPath().getName();
			
			try {
				JSONObject js = new JSONObject(line);
			
				if(js.has("user_id")){
					String user_id = js.optString("user_id");
					k2.set(user_id);
					v2.set("");
					context.write(k2, v2);
				}else {
					String _id = js.optString("_id");
				
						String name = js.optString("name");
						k2.set(_id);
						v2.set(name);
						
				
				}
				context.write(k2, v2);
			} catch (Exception e) {
				// TODO: handle exception
			}
				
			
			
		}
		
		
	}
	public static class weiboReduceOen extends Reducer<Text, Text, Text, LongWritable>{
		Text k3 = new Text();
		LongWritable v3 = new LongWritable();
		@Override
		protected void reduce(Text k2, Iterable<Text> v2,
				Reducer<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum=0;
			String name="";
			String line=null;
					for (Text text : v2) {
					String lin = text.toString();
						if("".equals(lin)){sum++;
						}else {
							name=lin;
						}
					
					}
			k3.set(name);
			v3.set(sum);
			context.write(k3, v3);
		}
	}
	public static class weiboMapTwo extends Mapper<LongWritable, Text, myLongWritable, Text>{
		//LongWritable k2num = new LongWritable();
		myLongWritable k2text = new myLongWritable();
		Text v2text = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, myLongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			long parseLong =0;
		
				parseLong=Long.parseLong(split[1]);
			
			k2text.set(parseLong);
			v2text.set(split[0]);
			context.write(k2text, v2text);
		}
	}
	public static class weiboReducerTwo extends Reducer<myLongWritable, Text, myLongWritable, Text>{
		
		@Override
		protected void reduce(myLongWritable k2, Iterable<Text> v2,
				Reducer<myLongWritable, Text, myLongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text name : v2) {
				context.write(k2,name );
			}
			
		
		}
	}
	static class myLongWritable extends LongWritable{
	@Override
	public int compareTo(LongWritable o) {
		// TODO Auto-generated method stub
		return -super.compareTo(o);
	}
		

}
}