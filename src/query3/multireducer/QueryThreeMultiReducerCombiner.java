package query3.multireducer;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Combiner class for query three.
 * It selects the latest modified date before the time threshold of all records of an article in the particular split.
 * 
 * @author vincentfung13
 */

public class QueryThreeMultiReducerCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	private Map<LongWritable, Entry<String, Date>> articleIDRevisions;
	
	@Override
	public void setup(Context context) {
		articleIDRevisions = new HashMap<LongWritable, Entry<String, Date>>();
	}
	
	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		for (Text value: values) {
			String[] revisionIDTimestamp = value.toString().split(" ");
			Date revisionDate = ISO8601Utils.parse(revisionIDTimestamp[1]);
			
			if (articleIDRevisions.containsKey(key)) {
				if (articleIDRevisions.get(key).getValue().before(revisionDate)) {
					articleIDRevisions.put(key, new AbstractMap.SimpleEntry<String, Date>(revisionIDTimestamp[0], revisionDate));
				}
			}
			else {
				articleIDRevisions.put(key, new AbstractMap.SimpleEntry<String, Date>(revisionIDTimestamp[0], revisionDate));
			}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		Iterator<Entry<LongWritable, Entry<String, Date>>> itr = articleIDRevisions.entrySet().iterator();
		while (itr.hasNext()) {
			Entry<LongWritable, Entry<String, Date>> currentEntry = itr.next();
			
			StringBuilder sb = new StringBuilder();
			sb.append(currentEntry.getValue().getKey());
			sb.append(" ");
			sb.append(ISO8601Utils.format(currentEntry.getValue().getValue()));
			
			context.write(currentEntry.getKey(), new Text(sb.toString()));
		}
	}
}
