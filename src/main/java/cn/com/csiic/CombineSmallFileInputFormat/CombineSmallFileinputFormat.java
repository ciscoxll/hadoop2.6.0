package cn.com.csiic.CombineSmallFileInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineSmallFileinputFormat extends CombineFileInputFormat<LongWritable, Text>{
	

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		//为什么使用CombineFileRecordReader的该构造方法？
		/**
		 * 如果有大量的小文件作为输入源，优先使用SequenceFileInputFormat？
		 * 只有存在SequenceFile格式的输入源时，才可以使用SequnceFileInputFormat。
		 * 如果没有SequenceFile格式的输入源，只能使用CombineFileInputFormat。
		 */
		return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit)split, context, CombineSmallFileRecordReader.class);
	}
}
