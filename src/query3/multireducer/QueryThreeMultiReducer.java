package query3.multireducer;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Reducer class for the multi-reducer version of query three.
 * 
 * @author vincentfung13
 */

public class QueryThreeMultiReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	@Override
	public void reduce (LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		Date currentLatestRevisionDate = null;
		String currentLatestRevisionID = "";
		
		for (Text value: values) {
			String[] revisionIDTimestamp = value.toString().split(" ");
			Date revisionDate = ISO8601Utils.parse(revisionIDTimestamp[1]);
			String revisionID = revisionIDTimestamp[0];
			
			if (currentLatestRevisionDate == null || revisionDate.after(currentLatestRevisionDate)) {
				currentLatestRevisionDate = revisionDate;
				currentLatestRevisionID = revisionID;
			}
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append(currentLatestRevisionID);
		sb.append(" ");
		sb.append(ISO8601Utils.format(currentLatestRevisionDate));
		
		context.write(key, new Text(sb.toString()));
	}

}
