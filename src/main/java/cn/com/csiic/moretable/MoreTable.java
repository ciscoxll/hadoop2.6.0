package cn.com.csiic.moretable;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MoreTable {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(MoreTable.class);
		
		job.setMapperClass(mtablemap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(MoreTablereduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path output = new Path(args[1]);
		output.getFileSystem(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
	}
	
	public static class mtablemap extends Mapper<LongWritable, Text, Text, Text>{
		/**factory： 											address： 	
			factoryname 　　　　	      addressed 					addressID addressname 	
			Beijing Red Star 　　　　				1								1 　　　　Beijing	
			Shenzhen Thunder 　　　　			3								2 　　　　Guangzhou	
			Guangzhou Honda 　　　　			2								3 　　　　Shenzhen	
			Beijing Rising 　　　　					1								4 　　　　Xian	
			Guangzhou Development Bank  2						
			Tencent 　　　　　　　　				3						
			Back of Beijing 　　　　				1						

		 * 
		 */
		Text k2 = new Text();
		Text v2 = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			FileSplit inputSplit =(FileSplit) context.getInputSplit();
			String filename = inputSplit.getPath().getName();
			String[] split = value.toString().split("\t");
			
			//if("f.txt".equals(filename)){
				String fid = split[1];
				k2.set(fid);
				v2.set(filename+"-->"+split[0]+"-->"+split[1]);
				if("f.txt".equals(filename))
				context.write(k2, v2);
				//}
			
				String aid = split[0];
				k2.set(aid);
				v2.set(filename+"-->"+split[0]+"-->"+split[1]);
				if("a.txt".equals(filename))
				context.write(k2, v2);
			
		}
		
	}
	public static class MoreTablereduce extends Reducer<Text, Text, Text, Text>{
		public static int times=0;
		Text k3 = new Text();
		Text v3 = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String f = "f.txt";
			String d = "a.txt";
			if(times==0){
				context.write(new Text("factoryname"), new Text("addressname"));
				times++;
			}
			ArrayList<String> factory = new ArrayList<String>();
			ArrayList<String> address = new ArrayList<String>();
			for (Text text : value) {
				String[] split = text.toString().split("-->");
					if(f.equals(split[0])){
						factory.add(split[1]);	
					}else {
						address.add(split[2]);
					}
			}
			if(factory.size()!=0&&address.size()!=0){
				for (int i = 0; i < factory.size(); i++) {
						for (int j = 0; j < address.size(); j++) {
							k3.set(factory.get(i));
							v3.set(address.get(j));
							context.write(k3, v3);
						}
				}
			}
		}
	}

}
