package query2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner class for query two
 * It aggregates the output from the mappers before sending the results to the reducer
 * 
 * @author vincentfung13
 *
 */
public class QueryTwoCombiner extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	
	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		long totalCounts = 0L;	
		for (LongWritable value: values)
			totalCounts += value.get();
		
		context.write(key, new LongWritable(totalCounts));
	}
}
