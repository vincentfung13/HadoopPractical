package query1;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Mapper class for query one
 * With input as <positionInFile, revisionContent> and output as <articleId, revisionId>
 * 
 * @author vincentfung13
 */
public class QueryOneMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
{
	private Date earlierDate, laterDate;
	private LongWritable articleId, revisionId;
	
	@Override 
	public void setup(Context context) 
	{
		Configuration conf = context.getConfiguration();
		//parameters
		earlierDate = ISO8601Utils.parse(conf.get("earlierTimestamp"));
		laterDate = ISO8601Utils.parse(conf.get("laterTimestamp"));
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
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
		                                 
		if(revisionDate.after(earlierDate) && revisionDate.before(laterDate)) {
				articleId = new LongWritable(Long.parseLong(firstLine[1]));
				revisionId = new LongWritable(Long.parseLong(firstLine[2]));     
				context.write(articleId, revisionId);
		}
	}
}
