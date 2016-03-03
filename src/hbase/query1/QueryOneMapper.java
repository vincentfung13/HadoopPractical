package hbase.query1;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;

/**
 * Mapper class for query one
 * With input as <positionInFile, revisionContent> and output as <articleId, revisionId>
 * 
 * @author vincentfung13
 */
public class QueryOneMapper extends TableMapper<LongWritable, LongWritable> {
	private LongWritable articleId, revisionId;
	
	public void map(ImmutableBytesWritable key, Result  value, Context context) throws IOException, InterruptedException {
		String articleIDRevID = new String(key.get());
		int stringLength = articleIDRevID.length();
		
		String revID = articleIDRevID.substring(stringLength - 9, stringLength - 1);
		String articleID = articleIDRevID.substring(0, stringLength - 10);
		this.articleId  = new LongWritable(Long.parseLong(articleID));
		this.revisionId = new LongWritable(Long.parseLong(revID));
		
		context.write(articleId, revisionId);
	}
}
