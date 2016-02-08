package query2;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Single reducer that selects the top K record and writes them to the output
 * @author vincentfung13
 *
 */

public class QueryTwoReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	
	private TreeMap<Long, PriorityQueue<Long>> modificationCountToArticle;
	private int k;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//parameters
		k = conf.getInt("k", 10);
		modificationCountToArticle = new TreeMap<Long, PriorityQueue<Long>>();
	}
	
	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		long count = 0L;
		for (LongWritable value: values){
			count += value.get();
		}
		
		PriorityQueue<Long> articleIdQueue;
		if (modificationCountToArticle.size() < k) {
			if (modificationCountToArticle.containsKey(count)) {
				articleIdQueue = modificationCountToArticle.get(count);			
			}
			else {
				articleIdQueue = new PriorityQueue<Long>();
			}
			articleIdQueue.add(key.get());
			modificationCountToArticle.put(count, articleIdQueue);
		}
		else if (modificationCountToArticle.size() == k) {
			if (modificationCountToArticle.firstKey() < count) {
				if (modificationCountToArticle.containsKey(count)) {
					articleIdQueue = modificationCountToArticle.get(count);			
				}
				else {
					articleIdQueue = new PriorityQueue<Long>();
				}
				articleIdQueue.add(key.get());
				modificationCountToArticle.remove(modificationCountToArticle.firstKey());
				modificationCountToArticle.put(count, articleIdQueue);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Iterator<Long> itr = modificationCountToArticle.descendingKeySet().iterator();
		while (itr.hasNext()) {
			long key = itr.next();
			System.out.println("Key:" + key);
			PriorityQueue<Long> articleIdQueue = modificationCountToArticle.get(key);
			while (articleIdQueue.size() > 0) {
				context.write(new LongWritable(articleIdQueue.poll()), new LongWritable(key));
			}
		}
	}
	
}