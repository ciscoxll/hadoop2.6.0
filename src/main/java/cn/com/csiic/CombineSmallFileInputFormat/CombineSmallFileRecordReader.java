package cn.com.csiic.CombineSmallFileInputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 读取每一个小文件的内容
 * @author Think
 *
 * @param <LongWritable>
 * @param <Text>
 */
public class CombineSmallFileRecordReader extends RecordReader<LongWritable, Text>{
	//前面讲过的专门用于读取每一行文本内容的RecordReader
	private LineRecordReader lrr;
	private CombineFileSplit combineFileSplit;
	private Integer index;
	
	public CombineSmallFileRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index){
		this.combineFileSplit = combineFileSplit;
		this.index = index;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.lrr = ReflectionUtils.newInstance(LineRecordReader.class, context.getConfiguration());
		
		Path file = combineFileSplit.getPath(index);
		long start = combineFileSplit.getOffset(index);
		long length = combineFileSplit.getLength(index);
		String[] hosts;
		try {
			hosts = combineFileSplit.getLocations();
			FileSplit fileSplit = new FileSplit(file, start, length, hosts);
			lrr.initialize(fileSplit , context);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return lrr.nextKeyValue();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return lrr.getCurrentKey();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return lrr.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lrr.getProgress();
	}

	@Override
	public void close() throws IOException {
		if(lrr!=null)lrr.close();
	}


}
