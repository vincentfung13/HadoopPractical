package query3.multireducer;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Mapper class for multi-reducer solution of query three.
 * It simply outputs <articleId, (revisionId, timestamp)> where timestamp is not later then the time threshold specified by the user.
 * 
 * @author vincentfung13
 */
public class QueryThreeMutiReducerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	Date timeThreshold;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		timeThreshold = ISO8601Utils.parse(conf.get("timestamp"));
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplit = line.split("\n");
		String[] firstLine = lineSplit[0].split(" ");
		StringTokenizer st = new StringTokenizer(lineSplit[0]);
		
		long articleId = Long.parseLong(firstLine[1]);
		String revisionId = firstLine[2];
		String timestamp = firstLine[4];
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
		
		if (revisionDate.before(timeThreshold)) {
			context.write(new LongWritable(articleId), new Text(revisionId + '\t' + timestamp));
		}
	}
}
