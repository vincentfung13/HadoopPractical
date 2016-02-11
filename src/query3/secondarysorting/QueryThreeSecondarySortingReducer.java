package query3.secondarysorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for the single-reducer solution of query 3
 * Note that it only outputs the first value because all the values all already sorted when they reach the reducers
 * @author vincentfung13
 *
 */
public class QueryThreeSecondarySortingReducer extends Reducer<ArticleIDTimestampWritable, IntWritable, ArticleIDTimestampWritable, Text> {
	
	@Override
	public void reduce(ArticleIDTimestampWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		// Output only the first record (the one with the latest timestamp)
		int i = 0;
		for (IntWritable value: values) {
			if (i == 1)
				break;
			StringBuilder sb = new StringBuilder();
			sb.append(value.get() + " ");
			sb.append(key.getTimeStamp());
			context.write(key, new Text(sb.toString()));
			i++;
		}
	}

}
