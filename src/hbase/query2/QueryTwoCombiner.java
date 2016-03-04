package hbase.query2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner class for query two
 * It aggregates the output from the mappers before sending the results to the reducer
 * 
 * @author vincentfung13
 */
public class QueryTwoCombiner extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
	
	@Override
	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		int totalCounts = 0;	
		for (IntWritable value: values)
			totalCounts += value.get();
		
		context.write(key, new IntWritable(totalCounts));
	}
}
