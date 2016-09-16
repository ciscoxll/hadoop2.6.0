package cn.com.csiic.cache;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 在map函数中第三方的jar包
 * @author Think
 *
 */
public class CacheJarsApp  extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		//帮助文档
		if(args==null ||args.length!=3){
			System.err.println("参数必须有3个，分别是[inputPath]   [outputPath]   [flag]");
			System.exit(-1);
		}
		
		String inputPath = args[0];
		Path outputDir = new Path(args[1]);
		String flag = args[2];

		//new一个对象
		Configuration conf = getConf();
		//删除输出目录
		outputDir.getFileSystem(conf).delete(outputDir, true);
		
		//job的名字，可以在以后方便的查找
		String jobName =  CacheJarsApp.class.getSimpleName();
		//job对象，用于组织所有的代码，统一提交给Yarn执行
		Job job = Job.getInstance(conf , jobName );
		
		//如果要打成jar包运行，必须有如下一行的代码，指定mainClass类
		job.setJarByClass(CacheJarsApp.class);
		
		//使用刚才上传到hdfs的外部jar文件
		if("1".equals(flag)){
			job.addArchiveToClassPath(new Path("/mysql-jdbc.jar"));
		}
		
		//需要知道输入的数据源在哪里
		FileInputFormat.setInputPaths(job, inputPath);
		//需要知道结果输出到哪里
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//使用自定义的Mapper类
		job.setMapperClass(WordCountMapper.class);
		
		//说明一下<k2,v2>的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//设置reduce task数量=0，表示job没有reduce task
		job.setNumReduceTasks(0);

		//在这里提交给yarn运行
		return job.waitForCompletion(true)?0:1;
	}
	/**
	 * 驱动方法
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CacheJarsApp(), args);
	}
	
	/**
	 * 第一阶段是自定义Mapper，继承org.apache.hadoop.mapreduce.Mapper，继承时的泛型表示<k1类型，v1类型，k2类型，v2类型>
	 * 
	 * 在这个类中，处理每一个<k1,v1>。处理逻辑是把每一行的内容解析成一个个的单词，然后输出单词和出现次数。
	 * @author Think
	 *
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		Logger logger = LoggerFactory.getLogger(WordCountMapper.class);

		@Override
		protected void setup(
				Mapper<LongWritable, org.apache.hadoop.io.Text, org.apache.hadoop.io.Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				Class.forName("com.mysql.jdbc.Driver");
				System.out.println("调用setup(...),读取驱动成功");
			} catch (ClassNotFoundException e) {
				System.out.println("调用setup(...),加载驱动失败");
				e.printStackTrace();
			}
		}
		
		
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();
		/**
		 * map函数的调用次数 等于 <k1,v1>的个数
		 */
		int times = 0;
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//把Text类型转化为java的String类型，为什么？因为String类中有切分字符串的方法
			String line = value.toString();
			logger.info("map({},{})运行第{}次", key.get(), line, ++times);
			System.out.println("map("+key.get()+","+line+")运行第{"+ (++times) +"}次");
			//原始文件中的单词使用\t分隔的。
			String[] splited = line.split("\t");
			for (String word : splited) {
				//k2表示每个单词
				k2.set(word);
				//v2表示出现次数，常数1
				v2.set(1);
				//把<k2,v2>通过context对象的write(...)写出去
				context.write(k2 , v2);
				logger.info("map({},{})运行第{}次，输出内容是<{},{}>", key.get(), line, times, word, 1);
				System.out.println("map(...)运行第"+times+"次，输出内容是<"+word+",1>");
			}
		}
	}
	
}

