package query2;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Comparator;

/**
 * Single reducer that selects the top K record and writes them to the output
 * @author vincentfung13
 *
 */

public class QueryTwoReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	
	private PriorityQueue<Map.Entry<Long, Long>> articleModificationCount;
	private int k;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//parameters
		k = conf.getInt("k", 10);
		articleModificationCount = new PriorityQueue<Map.Entry<Long, Long>>(k, new ModificationCountComparator());
	}
	
	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		long count = 0L;
		
		for (LongWritable value: values){
			count += value.get();
		}
		
		/*
		 * Need to reconsider this logic
		 */
		if (articleModificationCount.size() < k) {
			Map.Entry<Long, Long> entry = new AbstractMap.SimpleEntry<Long, Long>(key.get(), count);
			articleModificationCount.add(entry);
		}
		else {
			long oldLeastValue = articleModificationCount.peek().getValue();
			if (count == oldLeastValue) {
				Map.Entry<Long, Long> entry = new AbstractMap.SimpleEntry<Long, Long>(key.get(), count);
				articleModificationCount.add(entry);
			}
			else if (count > oldLeastValue) {
				articleModificationCount.poll();
				Map.Entry<Long, Long> entry = new AbstractMap.SimpleEntry<Long, Long>(key.get(), count);
				articleModificationCount.add(entry);
			}
		}
		
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		
		while (articleModificationCount.size() > 0) {
			Map.Entry<Long, Long> entry = articleModificationCount.poll();
			context.write(new LongWritable(entry.getKey()), new LongWritable(entry.getValue()));
		}
		
		articleModificationCount = null;
	}
	
	private class ModificationCountComparator implements Comparator<Map.Entry<Long, Long>> {
		@Override
		public int compare(Map.Entry<Long, Long> o1,
				Map.Entry<Long, Long> o2) {
			return (int) (o2.getValue() - o1.getValue());
		}
	}
	
}
