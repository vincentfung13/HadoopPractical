package mapreduce.query3.secondarysorting;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Mapper class for single-reducer solution of query three
 * It simply outputs <composite(articleId, timestamp), revisionId> where timestamp is before the time threshold specified by the suer
 * 
 * @author vincentfung13
 */
public class QueryThreeSecondarySortingMapper extends Mapper<ArticleIDTimestampWritable, Text, ArticleIDTimestampWritable, IntWritable> {
	
	Date timeThreshold;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		timeThreshold = ISO8601Utils.parse(conf.get("timestamp"));
	}
	
	@Override
	public void map(ArticleIDTimestampWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplit = line.split("\n");
		String[] firstLine = lineSplit[0].split(" ");
		
		String articleId = firstLine[1];
		String revisionId = firstLine[2];
		String timestamp = firstLine[4];
		Date revisionDate = ISO8601Utils.parse(timestamp);
		
		if (revisionDate.before(timeThreshold)) {
			context.write(new ArticleIDTimestampWritable(Integer.parseInt(articleId), timestamp), new IntWritable(Integer.parseInt(revisionId)));
		}
	}
}
