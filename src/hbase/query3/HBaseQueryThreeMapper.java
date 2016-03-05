package hbase.query3;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Mapper class for single-reducer solution of query three
 * It simply outputs <composite(articleId, timestamp), revisionId> where timestamp is before the time threshold specified by the suer
 * 
 * @author vincentfung13
 */
public class HBaseQueryThreeMapper extends TableMapper<ArticleIDTimestampWritable, LongWritable> {
	
	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		long articleID = Bytes.toLong(key.get(), 0);
		long revID = Bytes.toLong(key.get(), 8);
		Date revDate = new Date(value.rawCells()[0].getTimestamp());
		context.write(new ArticleIDTimestampWritable(articleID, ISO8601Utils.format(revDate)), new LongWritable(revID));
	}
}
