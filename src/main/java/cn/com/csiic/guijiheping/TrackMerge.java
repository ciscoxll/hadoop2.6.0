package cn.com.csiic.guijiheping;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.interfaces.RSAKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class TrackMerge extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TrackMerge(), args);
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf =getConf();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(TrackMerge.class);
		
		job.setMapperClass(trackmap.class);
		
		job.setMapOutputKeyClass(NOandTime.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(trackreduce.class);
		job.setOutputKeyClass(NOandTime.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		Path output = new Path(args[1]);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		job.setGroupingComparatorClass(apgroping.class);
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static class trackmap extends Mapper<LongWritable, Text,NOandTime , Text>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NOandTime, Text>.Context context)
				throws IOException, InterruptedException {
			
				String[] split = value.toString().split("\\s+");
				String[] time = split[4].split("\\.");
				NOandTime nOandTime = new NOandTime(split[0],time[0],split[1]);
				context.write(nOandTime, value);
				
		
			
		}
	}
	public static class trackreduce extends Reducer<NOandTime, Text, NOandTime, Text>{
		Text v3 = new Text();
		@Override
		protected void reduce(NOandTime k2, Iterable<Text> v2,
				Reducer<NOandTime, Text, NOandTime, Text>.Context context)
				throws IOException, InterruptedException {
			long sum=0;
			long parseLong=0;
			String[] rs =null;
			for (Text text : v2) {
				rs = text.toString().split("\\s+");
				parseLong = Long.parseLong(rs[7]);
				sum+=parseLong;
			}
			v3.set(rs[2]+"\t"+rs[3]+"\t"+rs[4]+"\t"+rs[5]+"\t"+rs[6]+"\t"+sum+"\t"+rs[8]);
			context.write(k2, v3);
			
		}
		
	}
	public static class apgroping extends WritableComparator{
		public apgroping() {
			super(NOandTime.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			NOandTime aa=(NOandTime)a;
			NOandTime bb=(NOandTime)b;
			int num = aa.phonenum.compareTo(bb.phonenum);
			//int apnumm=aa.ap.compareTo(bb.ap);
			if(num==0){
			return aa.ap.compareTo(bb.ap);
			}else{
				return num;
			}
		}
	}
	static class NOandTime implements WritableComparable<NOandTime>{
		public String phonenum;
		public String starttime;
		public String ap;
		

		public NOandTime() {
			// TODO Auto-generated constructor stub
		}

		public NOandTime(String phonenum, String starttime, String ap) {
		
			this.phonenum = phonenum;
			this.starttime = starttime;
			this.ap = ap;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(phonenum);
			out.writeUTF(starttime);
			out.writeUTF(ap);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.phonenum=in.readUTF();
			this.starttime=in.readUTF();
			this.ap=in.readUTF();
		}

		@Override
		public int compareTo(NOandTime o) {
			int phone = this.phonenum.compareTo(o.phonenum);
			int time = this.starttime.compareTo(o.starttime);
			if(phone==0){
				return time;
			}
			return phone;
		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return this.phonenum+"\t"+this.ap;
		}
		
	}
	
}

