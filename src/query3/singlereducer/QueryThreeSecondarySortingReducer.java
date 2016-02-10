package query3.singlereducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueryThreeSecondarySortingReducer extends Reducer<ArticleIDTimestampWritable, LongWritable, ArticleIDTimestampWritable, Text> {
	
	@Override
	public void reduce(ArticleIDTimestampWritable key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		// Output only the first record (the one with the latest timestamp)
		int i = 0;
		for (LongWritable value: values) {
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
