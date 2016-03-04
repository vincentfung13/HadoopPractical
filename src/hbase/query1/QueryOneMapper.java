package hbase.query1;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

/**
 * Mapper class for query one
 * With input as <positionInFile, revisionContent> and output as <articleId, revisionId>
 * 
 * @author vincentfung13
 */
public class QueryOneMapper extends TableMapper<LongWritable, LongWritable> {
	
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		long articleID = Bytes.toLong(key.get(), 0);
		long revID = Bytes.toLong(key.get(), 8);
		
		context.write(new LongWritable(articleID), new LongWritable(revID));
	}
}
