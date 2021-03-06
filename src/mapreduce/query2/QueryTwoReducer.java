package mapreduce.query2;

import java.io.IOException;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Single reducer that selects the top K record and writes them to the output.
 * It makes use of a treemap which stores <key, value> pair where key is modification count and value is a priority queue of article IDs.
 * This data structure conveniently handles the sorting and key-value retrieval. It also handles the case where many article may share the same modification count.
 * 
 * @author vincentfung13
 */

public class QueryTwoReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	private TreeMap<Integer, PriorityQueue<Integer>> modificationCountToArticle;
	private int k;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 10);
		modificationCountToArticle = new TreeMap<Integer, PriorityQueue<Integer>>();
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		int count = 0;
		for (IntWritable value: values){
			count += value.get();
		}
		
		PriorityQueue<Integer> articleIdQueue;
		if (modificationCountToArticle.size() < k) {
			if (modificationCountToArticle.containsKey(count)) {
				articleIdQueue = modificationCountToArticle.get(count);			
			}
			else {
				articleIdQueue = new PriorityQueue<Integer>();
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
					articleIdQueue = new PriorityQueue<Integer>();
				}
				articleIdQueue.add(key.get());
				modificationCountToArticle.remove(modificationCountToArticle.firstKey());
				modificationCountToArticle.put(count, articleIdQueue);
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Iterator<Integer> itr = modificationCountToArticle.descendingKeySet().iterator();
		while (itr.hasNext()) {
			int key = itr.next();
			PriorityQueue<Integer> articleIdQueue = modificationCountToArticle.get(key);
			while (articleIdQueue.size() > 0) {
				context.write(new IntWritable(articleIdQueue.poll()), new IntWritable(key));
			}
		}
	}
	
}