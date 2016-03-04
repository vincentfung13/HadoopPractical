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
	
	public void map(ImmutableBytesWritable key, Result  value, Context context) throws IOException, InterruptedException {
		String articleIDRevID = Bytes.toString(key.get());
		int stringLength = articleIDRevID.length();
		String revID = articleIDRevID.substring(stringLength - 9, stringLength - 1);
		String articleID = articleIDRevID.substring(0, stringLength - 10);
		
		context.write(new LongWritable(Long.parseLong(articleID)), new LongWritable(Long.parseLong(revID)));
	}
}
