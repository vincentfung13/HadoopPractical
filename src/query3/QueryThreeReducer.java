package query3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueryThreeReducer extends Reducer<ArticleIDTimestampWritable, LongWritable, ArticleIDTimestampWritable, Text> {
	
	@Override
	public void reduce(ArticleIDTimestampWritable key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		for (LongWritable value: values) {
			StringBuilder sb = new StringBuilder();
			sb.append(value.get() + " ");
			sb.append(key.getTimeStamp());
			
			context.write(key, new Text(sb.toString()));
		}
	}

}
