package cn.com.csiic.pv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Access extends Configured implements Tool {
	public static String formatDate(long date) {
		return new SimpleDateFormat("dd/MMM/yyyy:kk:mm:ss", Locale.ENGLISH).format(new Date(date));
	}
	public static Date format(String dateString) {
		try {
			return (new SimpleDateFormat("dd/MMM/yyyy:kk:mm:ss",Locale.ENGLISH)).parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Access(), args);
	}
	public static class IPAccessMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split(" - - \\[");
			Date format = format(split[1]);
			k2.set(split[0]);
			v2.set(format.getTime());
			context.write(k2, v2);
			System.out.println(k2+":"+v2);
		}
	}
	
	public static class IPAccessReducer extends Reducer<Text, LongWritable, Text, Text>{

		Text v3=new Text();
		
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			TreeSet<Long> treeSet=new TreeSet<>();
			for (LongWritable longWritable : v2s) {
				long e = longWritable.get();
				System.out.println("迭代中："+e);
				treeSet.add(e);
			}
			System.out.println(k2+":"+treeSet);
			int count=0;
			long minTime=0;
			for (Long currentTime : treeSet) {
				if (++count==1) 
					minTime=currentTime;
				long time = currentTime-minTime;
				System.out.println("currentTime.get()-minTime="+time);
				System.out.println("count="+count);
				if(time>=30*60*1000){
					v3.set(formatDate(minTime)+"\t"+formatDate(currentTime)+"\t"+time);
					minTime=currentTime;
					context.write(k2, v3);
				}
			}
		}
	}
	

	
	static class IPTime implements WritableComparable<IPTime>{
		String ip;
		long time;
		
		public void setField(String ip, long time) {
			this.ip = ip;
			this.time = time;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(ip);
			out.writeLong(time);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			ip=in.readUTF();
			time=in.readLong();
		}
		@Override
		public int compareTo(IPTime o) {
			int ipcom = ip.compareTo(o.ip);
			if(ipcom==0)
				return (int) (time-o.time);
			return ipcom;
		}
		
	}



	@Override
	public int run(String[] args) throws Exception {
		if (args==null||args.length!=2) {
			System.err.println("参数必须有2个，分别是[inputPath]   [outputPath]");
			return -1;
		}
		Configuration conf = getConf();
		Path outputDir = new Path(args[1]);
		outputDir.getFileSystem(conf).delete(outputDir,true);
		
		Job job = Job.getInstance(conf, "IPAccess");
		job.setJarByClass(Access.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(IPAccessMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
//		job.setCombinerClass(IntSumReducer.class);
		
		job.setReducerClass(IPAccessReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true)?0:-1;
		
	}
}
