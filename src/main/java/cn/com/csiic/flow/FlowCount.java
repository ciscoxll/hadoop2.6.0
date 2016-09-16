package cn.com.csiic.flow;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FlowCount {
	final static String INPUT="hdfs://master64:9000/flowin";
	final static String OUTPUT="hdfs://master64:9000/flowout";
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
			Job job = Job.getInstance();
			job.setJarByClass(FlowCount.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(myflow.class);
			
			job.setOutputValueClass(Text.class);
			job.setOutputValueClass(myflow.class);
			
			
			job.setReducerClass(flowreduce.class);
			job.setMapperClass(flowmap.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputPaths(job, new Path(INPUT));
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
			job.waitForCompletion(true);
		
	}
	static class flowmap extends Mapper<LongWritable, Text, Text, myflow>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
				String[] split = value.toString().split("\\s");
				String phonenum = split[1];
				myflow myflow = new myflow(Long.parseLong(split[8]),Long.parseLong(split[9]));
				context.write(new Text(phonenum), myflow);
		}
		
	}
	static class flowreduce extends Reducer<Text, myflow, Text, myflow>{
		@Override
		protected void reduce(Text key3, Iterable<myflow> values3,
				Reducer<Text, myflow, Text, myflow>.Context context)
				throws IOException, InterruptedException {
			long sumup=0;
			long sumdowm=0;
			for (myflow count : values3) {
				sumup=count.getUpflow();
				sumdowm=count.getDownflow();
			}
			context.write(key3,new myflow(sumup, sumdowm));
			
		}
	}

}


/**
 * hadoop中序列化框架的应用示例
 * 反序列化时，反射机制会调用类的无参构造函数
 * 所以，如果你在类中定义了有参构造，就一定要记得显式定义一下无参构造函数
 * @author
 *
 */
class myflow implements WritableComparable<myflow>{
	private long upflow;
	private long downflow;
	private long sumflow;
	/**
	 * 显式定义无参构造
	 */
	public myflow() {
		
	}
	public myflow(long up,long down){
		this.upflow=up;
		this.downflow=down;
		this.sumflow=up+down;
		
	}
	
	public long getUpflow() {
		return upflow;
	}

	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}

	public long getDownflow() {
		return downflow;
	}

	public void setDownflow(long downflow) {
		this.downflow = downflow;
	}

	public long getSumflow() {
		return sumflow;
	}

	public void setSumflow(long sumflow) {
		this.sumflow = sumflow;
	}



	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upflow);
		out.writeLong(downflow);
		out.writeLong(sumflow);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upflow = in.readLong();
		this.downflow = in.readLong();
		this.sumflow=in.readLong();
		
		
	}



	@Override
	public int compareTo(myflow o) {
		//实现按照sumflow的大小倒序排序
		//return sumflow>o.getSumflow()?-1:1;
		return sumflow>o.getSumflow()?-1:1;
	}
	@Override
	public String toString() {
		return upflow+"\t"+downflow+"\t"+sumflow;
	}

}
