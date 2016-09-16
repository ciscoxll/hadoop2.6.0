package cn.com.csiic.Sequence;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
/**
 * 把大量小文件合并成大文件写入HDFS 	以<k,v>形式  k文件名 v文件内容
 * @author xiaoliangliang
 *SequeceFile是Hadoop API提供的一种二进制文件。这种二进制文件直接将<key, value>对序列化到文件中。一般对小文件可以使用这种文件合并，即将文件名作为key，文件内容作为value序列化到大文件中。这种文件格式有以下好处：
	支持压缩，且可定制为基于Record或Block压缩（Block级压缩性能较优）
	本地化任务支持：因为文件可以被切分，因此MapReduce任务时数据的本地化情况应该是非常好的。
	对key、value的长度进行了定义，(反)序列化速度非常快。
	缺点是需要一个合并文件的过程，文件较大，且合并后的文件将不方便查看，必须通过遍历查看每一个小文件。

 */
public class SequenceFileWriterApp {

	public static void main(String[] args) throws Exception {
		if(args.length!=3){
			System.err.println("参数必须是3个，分别是 inputDir  outputPath   compressionType");
			System.exit(-1);
		}
		
		String inputDir = args[0];
		Path outputPath = new Path(args[1]);
		String compType = args[2];	//压缩类型，如果值为1表示None，值为2表示Block，值为3表示Record
		
		Configuration conf = new Configuration();
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		//数组中有4个元素
		Option[] opts = new Option[4];
		//第1个元素是输出路径
		opts[0] = Writer.file(outputPath);
		//第2个元素是key类型
		opts[1] = Writer.keyClass(Text.class);
		//第3个元素是value类型
		opts[2] = Writer.valueClass(Text.class);
		//第4个元素是压缩类型
		CompressionType compressionType = getCompressionType(compType);
		CompressionCodec codec = new GzipCodec();
		opts[3] = Writer.compression(compressionType, codec);
		
		Writer writer = SequenceFile.createWriter(conf , opts);
		
		Text key = new Text();
		Text val = new Text();

		//遍历输入文件夹，获得所有文件的数组
		File[] listFiles = new File(inputDir).listFiles();
		//把每一个文件的名称作为key，文件内容作为value
		for (File file : listFiles) {
			//获取文件名
			String name = file.getName();
			key.set(name);
			//获取文件内容
			String content = FileUtils.readFileToString(file);
			val.set(content);
			//调用append()把key、val写入到SequenceFile中
			writer.append(key, val);
		}
		writer.close();
	}

	public static CompressionType getCompressionType(String compType) {
		if("2".equals(compType)){
			return CompressionType.BLOCK;
		}else if("3".equals(compType)){
			return CompressionType.RECORD;
		}else{
			return CompressionType.NONE;
		}
		
	}

}
