package utility;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;

public class WikiModificationFileInputFormat extends FileInputFormat<IntWritable, Text> {

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit split, 
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new RevisionEntryRecordReader();
	}
	
	private class RevisionEntryRecordReader extends RecordReader<IntWritable, Text>{
		
		private final byte[] recordSeparator = "\n\n".getBytes();
		private FSDataInputStream fsin; 
		private long start, end;
		private boolean stillInChunk = true;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private IntWritable key = new IntWritable(); 
		private Text value = new Text();

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public IntWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (float) (fsin.getPos() - start) / (end - start);
		}

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) inputSplit;
			Configuration conf = context.getConfiguration();
			Path path = split.getPath();
			FileSystem fs = path.getFileSystem(conf);

			fsin = fs.open(path);
			start = split.getStart();
			end = split.getStart() + split.getLength();
			fsin.seek(start);

			if (start != 0)
				readRecord(false);
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!stillInChunk)
				return false;
			
			boolean status = readRecord(true);
			value = new Text();
			value.set(buffer.getData(), 0, buffer.getLength());
			
			int articleId = Integer.parseInt(value.toString().split(" ")[1]);
			key.set(articleId);
			
			buffer.reset();
			if (!status)
				stillInChunk = false; 
			return true;
		}
		
		private boolean readRecord(boolean withinBlock) throws IOException {
			int i = 0, b;
			while (true) {
				if ((b = fsin.read()) == -1)
					return false;
				
				if (withinBlock)
					buffer.write(b);
			
				if (b == recordSeparator[i]) {
					if (++i == recordSeparator.length) 
						return fsin.getPos() < end;
				} 
				else
					i = 0;
			}
		}

	}

}
