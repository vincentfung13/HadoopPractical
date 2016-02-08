package query3;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/*
 * Mapper class for query one
 * With input as <positionInFile, revisionContent> and output as <composite(articleId, timestamp), revisionId>
 */
public class QueryThreeMapper extends Mapper<LongWritable, Text, ArticleIDTimestampWritable, LongWritable> {
	
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
		
		String articleId = firstLine[1];
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
			context.write(new ArticleIDTimestampWritable(Long.parseLong(articleId), timestamp), new LongWritable(Long.parseLong(revisionId)));
		}
	}
}
