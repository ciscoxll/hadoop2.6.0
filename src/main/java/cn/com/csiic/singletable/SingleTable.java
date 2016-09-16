package cn.com.csiic.singletable;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ext.JodaDeserializers;
import org.junit.Test;



public class SingleTable {
	public static int time=0;
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SingleTable.class);
		
		job.setMapperClass(singlemap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(singlereduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//
		//job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path output = new Path(args[1]);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);

	}
	public static class singlemap extends Mapper<LongWritable, Text,Text, Text>{
		/*child parent 
			Tom Lucy
			Tom Jack
			Jone Lucy
			Jone Jack
			Lucy Mary
			Lucy Ben
			Jack Alice
			Jack Jesse
			Terry Alice
			Terry Jesse
			Philip Terry
			Philip Alma
			Mark Terry
			Mark Alma

      */

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\\s");
			String table1="1";
			String table2="2";
			if(split[0].compareTo("child")!=0){
				String childname = split[0];
				String parent = split[1];
				context.write(new Text(split[1]), new Text(table1+"-->"+childname+"-->"+parent));
				context.write(new Text(split[0]), new Text(table2+"-->"+childname+"-->"+parent));
				
			}
			
			
		}
	}
	public static class singlereduce extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text k3, Iterable<Text> v3,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(time==0){
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			String[] child1=new String[10];
			String[] parent2=new String[10];
			int t1time=0;
			int t2time=0;
			StringBuffer strbuf = new StringBuffer();
			for (Text text : v3) {
				
				String[] split = text.toString().split("-->");
				if(split[0].equals("1")){ child1[t1time]=split[1];t1time++;}
				if(split[0].equals("2")){parent2[t2time]=split[2];t2time++;}
			}
			
			if(t1time!=0&&t2time!=0){
				for (int i = 0; i <t1time; i++) {
					for (int j = 0; j <t2time; j++) {
						context.write(new Text(child1[i]), new Text(parent2[j]));
					}
	           }
			}
			//context.write(k3, new Text(strbuf.toString()));
			
		}
		
	}
	

}

