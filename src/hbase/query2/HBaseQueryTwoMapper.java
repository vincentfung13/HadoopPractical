package hbase.query2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Mapper class for query two.
 * It is similar to the one for query one, except it outputs <articleId, numberOfModifications> for each revision record.
 * Note that each mapper aggregates its pairs based on keys first before sending them to the combiner. 
 * 
 * @author vincentfung13
 */

public class HBaseQueryTwoMapper extends TableMapper<LongWritable, IntWritable> {
	private IntWritable modificationCounts;
	private Map<Long, Integer> articleModificationCounts;
	
	@Override
	public void setup(Context context) {
		articleModificationCounts = new HashMap<Long, Integer>();
	}
	
	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		// Trade off between memory usage and efficiency
		long articleID = Bytes.toLong(key.get(), 0);
		if (articleModificationCounts.containsKey(articleID)) {
			int oldValue = articleModificationCounts.get(articleID);
			articleModificationCounts.put(articleID, oldValue + 1);
		}
		else {
			articleModificationCounts.put(articleID, 1);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Iterator<Entry<Long, Integer>> itr = articleModificationCounts.entrySet().iterator();
		while (itr.hasNext()){
			Entry<Long, Integer> entry = itr.next();
			modificationCounts = new IntWritable(entry.getValue());
			context.write(new LongWritable(entry.getKey()), modificationCounts);
		}
		// Clean up the map
		articleModificationCounts = null;
	}
}
