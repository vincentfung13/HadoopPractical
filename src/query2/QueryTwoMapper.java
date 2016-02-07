package query2;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Mapper class for query two.
 * It is similar to the one for query one, except it outputs <articleId, numberOfModifications> for each revision record.
 * @author vincentfung13
 *
 */

public class QueryTwoMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	
	private Date earlierDate, laterDate;
	private LongWritable articleIdWritable, modificationCounts;
	private Map<Long, Long> articleModificationCounts;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		//parameters
		earlierDate = ISO8601Utils.parse(conf.get("earlierTimestamp"));
		laterDate = ISO8601Utils.parse(conf.get("laterTimestamp"));
		articleModificationCounts = new HashMap<Long, Long>();
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplit = line.split("\n");
		String[] firstLine = lineSplit[0].split(" ");
		StringTokenizer st = new StringTokenizer(lineSplit[0]);
		
		Date revisionDate = null;
		while (st.hasMoreTokens()){
			String str = st.nextToken();
			try {
				revisionDate = ISO8601Utils.parse(str);
				break;
			}
			catch(IllegalArgumentException e){
				continue;
			}
		}
		        
		// Trade off between memory usage and efficiency
		if (revisionDate.after(earlierDate) && revisionDate.before(laterDate)) {
				long articleId = Long.parseLong(firstLine[1]);  
				if (articleModificationCounts.containsKey(articleId)) {
					long oldValue = articleModificationCounts.get(articleId);
					articleModificationCounts.put(articleId, oldValue + 1);
				}
				else {
					articleModificationCounts.put(articleId, 1L);
				}
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		Iterator<Entry<Long, Long>> itr = articleModificationCounts.entrySet().iterator();
		while (itr.hasNext()){
			Entry<Long, Long> entry = itr.next();
			articleIdWritable = new LongWritable(entry.getKey());
			modificationCounts = new LongWritable(entry.getValue());
			context.write(articleIdWritable, modificationCounts);
		}
		
		// Clean up the map
		articleModificationCounts = null;
	}
}
