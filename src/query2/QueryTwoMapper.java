package query2;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Mapper class for query two.
 * It is similar to the one for query one, except it outputs <articleId, numberOfModifications> for each revision record.
 * Note that each mapper aggregates its pairs based on keys first before sending them to the combiner. 
 * 
 * @author vincentfung13
 */

public class QueryTwoMapper extends Mapper<IntWritable, Text, IntWritable, IntWritable> {
	
	private Date earlierDate, laterDate;
	private IntWritable modificationCounts;
	private Map<IntWritable, Integer> articleModificationCounts;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//parameters
		earlierDate = ISO8601Utils.parse(conf.get("earlierTimestamp"));
		laterDate = ISO8601Utils.parse(conf.get("laterTimestamp"));
		articleModificationCounts = new HashMap<IntWritable, Integer>();
	}
	
	@Override
	public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplit = line.split("\n");
		String[] firstLine = lineSplit[0].split(" ");
		
		String timestamp = firstLine[4];
		Date revisionDate = ISO8601Utils.parse(timestamp);
		        
		// Trade off between memory usage and efficiency
		if (revisionDate.after(earlierDate) && revisionDate.before(laterDate)) { 
				if (articleModificationCounts.containsKey(key)) {
					int oldValue = articleModificationCounts.get(key);
					articleModificationCounts.put(key, oldValue + 1);
				}
				else {
					articleModificationCounts.put(key, 1);
				}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Iterator<Entry<IntWritable, Integer>> itr = articleModificationCounts.entrySet().iterator();
		while (itr.hasNext()){
			Entry<IntWritable, Integer> entry = itr.next();
			modificationCounts = new IntWritable(entry.getValue());
			context.write(entry.getKey(), modificationCounts);
		}
		
		// Clean up the map
		articleModificationCounts = null;
	}
}
