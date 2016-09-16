package cn.com.csiic.Partition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * v2、v3使用自定义Writable类型
 * @author Think
 *
 */
public class PartitionedTrafficStatApp {

	public static void main(String[] args) throws Exception {
		if(args.length!=3){
			System.err.println("参数必须是3个，分别是 <inputPath>   <outputPathDir>   <numReduceTasks>");
			System.exit(-1);
		}
		
		String inputPath = args[0];
		Path outputDir = new Path(args[1]);
		Integer numReduceTasks = Integer.parseInt(args[2]);
		
		Configuration conf = new Configuration();
		FileSystem fs = outputDir.getFileSystem(conf);
		fs.delete(outputDir, true);
		
		String jobName = PartitionedTrafficStatApp.class.getSimpleName();
		Job job = Job.getInstance(conf , jobName );
		job.setJarByClass(PartitionedTrafficStatApp.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir);
	//	job.setInputFormatClass(seq);
		
		job.setMapperClass(TrafficStatMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TrafficStatWritable.class);

		//指定自定义的分区类
		//job.setPartitionerClass(HashPartitioner.class);	默认的分区类
		job.setPartitionerClass(PhoneNumberPartitioner.class);
		//指定reduce task数量
		job.setNumReduceTasks(numReduceTasks);
		
		
		job.setReducerClass(TrafficStatReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TrafficStatWritable.class);
		
		job.waitForCompletion(true);		
	}

	
	public static class TrafficStatMapper extends Mapper<LongWritable, Text, Text, TrafficStatWritable>{
		Text k2 = new Text();
		TrafficStatWritable v2 = new TrafficStatWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, TrafficStatWritable>.Context context)
				throws IOException, InterruptedException {
			//读取每一行内容
			String line = value.toString();
			//拆分
			String[] splited = line.split("\t");
			//找出手机号、4个流量值
			String phoneNumber = splited[1];
			//k2赋值
			k2.set(phoneNumber);
			//v2赋值
			v2.setString(splited[6], splited[7], splited[8], splited[9]);
			//写出去
			context.write(k2, v2);
		}
	}
	
	
	public static class TrafficStatReducer extends Reducer<Text, TrafficStatWritable, Text, TrafficStatWritable>{
		TrafficStatWritable v3 = new TrafficStatWritable();

		@Override
		protected void reduce(Text k2, Iterable<TrafficStatWritable> v2s,
				Reducer<Text, TrafficStatWritable, Text, TrafficStatWritable>.Context context)
				throws IOException, InterruptedException {
			
			long upPackNum = 0;
			long downPackNum = 0;
			long upPayLoad = 0;
			long downPayLoad = 0;
			
			for(TrafficStatWritable v2 : v2s){
				upPackNum += v2.upPackNum;
				downPackNum += v2.downPackNum;
				upPayLoad += v2.upPayLoad;
				downPayLoad += v2.downPayLoad;
			}
			
			v3.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
			
			context.write(k2, v3);
		}
	}
	
	
	public static class TrafficStatWritable implements Writable{
		private Long upPackNum;
		private Long downPackNum;
		private Long upPayLoad;
		private Long downPayLoad;
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(this.upPackNum);
			out.writeLong(this.downPackNum);
			out.writeLong(this.upPayLoad);
			out.writeLong(this.downPayLoad);
		}

		public void setString(String upPackNum, String downPackNum, String upPayLoad, String downPayLoad) {
			this.set(Long.parseLong(upPackNum), Long.parseLong(downPackNum), Long.parseLong(upPayLoad), Long.parseLong(downPayLoad));
		}
		
		public void set(Long upPackNum, Long downPackNum, Long upPayLoad, Long downPayLoad) {
			this.upPackNum = upPackNum;
			this.downPackNum = downPackNum;
			this.upPayLoad = upPayLoad;
			this.downPayLoad = downPayLoad;
		}
		

		@Override
		public void readFields(DataInput in) throws IOException {
			this.upPackNum = in.readLong();
			this.downPackNum = in.readLong();
			this.upPayLoad = in.readLong();
			this.downPayLoad = in.readLong();
		}
		
		@Override
		public String toString() {
			return this.upPackNum+"\t"+this.downPackNum+"\t"+this.upPayLoad+"\t"+this.downPayLoad;
		}
	}
	
	
	public static class PhoneNumberPartitioner extends Partitioner<Text, TrafficStatWritable>{

		/**
		 * 返回值表示的是分区编号，从0开始的，1、2、3、4。简单认为是数组的索引
		 */
		@Override
		public int getPartition(Text key, TrafficStatWritable value, int numPartitions) {
			String phoeneNumber = key.toString();
			return phoeneNumber.length()==11?0:1;
		}
		
	}
}
