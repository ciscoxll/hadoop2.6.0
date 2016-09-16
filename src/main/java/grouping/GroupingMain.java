package grouping;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import sun.tools.tree.NewArrayExpression;
import sun.tools.tree.SuperExpression;



public class GroupingMain {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(GroupingMain.class);
		
		job.setMapperClass(GroupMap.class);
		job.setMapOutputKeyClass(myassess.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(groupingreduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//使用自定义的分组策略
		job.setGroupingComparatorClass(ipgrouping.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path output = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, output);
		output.getFileSystem(conf).delete(output, true);
		
		job.waitForCompletion(true);
	}
	public static String formatDate(long date) {
		return new SimpleDateFormat("dd/MMM/yyyy:kk:mm:ss", Locale.ENGLISH).format(new Date(date));
	}
	public static Date format(String date) throws Exception {
		return new SimpleDateFormat("dd/MMM/yyyy:kk:mm:ss", Locale.ENGLISH).parse(date);
	}
	public static class GroupMap extends Mapper<LongWritable, Text, myassess, Text>{
		myassess k2 = new myassess();
		Text v2 = new Text();
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, myassess, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("--\\[");
			String times = split[1];
			long parseLong=0;
			try {
				Date format = format(split[1]);
				parseLong=format.getTime();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			v2.set(split[0]+"\t"+split[1]);
			k2.setIp(split[0]);
			k2.setTime(parseLong);
			context.write(k2,v2 );
		}
		
	}
	static class groupingreduce extends Reducer<myassess, Text, Text, Text>{
		
		Text k3 = new Text();
		Text v3 = new Text();
		@Override
		protected void reduce(myassess k2, Iterable<Text> v2,
				Reducer<myassess, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		long parseLong=0;
			int num=0;
			long start=0;
			long end=0;
			for (Text text : v2) {
				String[] split = text.toString().split("\t");
			
					try {
						Date format = format(split[1]);
						parseLong=format.getTime();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(num==0){
						start=parseLong;
					}
				num++;
				end=parseLong;
			}
			String startdate = formatDate(start);
			String endDate = formatDate(end);
			long access=start-end;
			k3.set(k2.ip);
			v3.set("开始时间"+startdate+"结束时间"+endDate+"停留时间"+access/1000/60);
			context.write(k3, v3);
			//context.write(new Text(k2.toString()), new Text());
		}
	}
	/**
	 * 比较k2的时候，只比较user
	 * 
	 *
	 */
	public static class ipgrouping extends WritableComparator{
		public ipgrouping() {
			// TODO Auto-generated constructor stub
			super(myassess.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			myassess aa=(myassess)a;
			myassess bb=(myassess)b;
			return aa.ip.compareTo(bb.ip);
			
		}
	}
	static  class myassess implements WritableComparable<myassess>{
		String ip;
		long time;
		
		public myassess(String ip, long time) {
		
			this.ip = ip;
			this.time = time;
		}
		public myassess() {
			
		
		}
		

		public String getIp() {
			return ip;
		}


		public void setIp(String ip) {
			this.ip = ip;
		}


		public long getTime() {
			return time;
		}


		public void setTime(long time) {
			this.time = time;
		}


		@Override
		public void write(DataOutput out) throws IOException {
			
			out.writeUTF(ip);
			out.writeLong(time);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.ip = in.readUTF();
			this.time=in.readLong();
			
		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return ip+"\t"+time;
		}

		@Override
		public int compareTo(myassess o) {
			int ipnum = this.ip.compareTo(o.ip);
			long timenum = this.time-o.time;
			if(ipnum==0){
				return (int) (this.time-o.time);
			}else {
				return ipnum;
			}
			
		}
		
	}

}
