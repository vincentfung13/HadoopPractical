package query3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import utility.ArticleIDTimestampWritable;

public class QueryThreeReducer extends Reducer<LongWritable, ArticleIDTimestampWritable, LongWritable, ArticleIDTimestampWritable> {
	@Override
	public void setup(Context context) {
	}
	
	@Override
	public void reduce(LongWritable key, Iterable<ArticleIDTimestampWritable> values, Context context) 
			throws IOException, InterruptedException {
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
	}
}
