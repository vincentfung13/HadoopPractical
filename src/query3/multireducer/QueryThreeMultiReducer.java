package query3.multireducer;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Reducer/Combiner class for query three.
 * Combiner selects the latest modified date before the time threshold of all records of an article in the particular split.
 * The reducer is doing the same thing except it has global knowledge of a single article.
 * 
 * @author vincentfung13
 */

public class QueryThreeMultiReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	private LongWritable currentLatestArticleID;
	private String currentLatestRevisionID;
	private Date currentLatestDate;
	
	@Override
	public void setup(Context context) {
		currentLatestArticleID = new LongWritable();
		currentLatestRevisionID = "";
		currentLatestDate = new Date();
	}
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		for (Text value: values) {
			String[] revisionIDTimestamp = value.toString().split("\t");
			Date revisionDate = ISO8601Utils.parse(revisionIDTimestamp[1]);
			if (revisionDate.after(currentLatestDate)) {
				currentLatestDate = revisionDate;
				currentLatestArticleID = key;
				currentLatestRevisionID = revisionIDTimestamp[0];
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(currentLatestArticleID, new Text(currentLatestRevisionID + "\t" + ISO8601Utils.format(currentLatestDate)));
	}
}
