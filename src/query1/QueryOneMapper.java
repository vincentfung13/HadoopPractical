package query1;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Mapper class for query one
 * With input as <positionInFile, revisionContent> and output as <articleId, revisionId>
 * 
 * @author vincentfung13
 */
public class QueryOneMapper extends Mapper<IntWritable, Text, IntWritable, IntWritable>
{
	private Date earlierDate, laterDate;
	private IntWritable revisionId;
	
	@Override 
	public void setup(Context context) 
	{
		Configuration conf = context.getConfiguration();
		//parameters
		earlierDate = ISO8601Utils.parse(conf.get("earlierTimestamp"));
		laterDate = ISO8601Utils.parse(conf.get("laterTimestamp"));
	}
	
	public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] lineSplit = line.split("\n");
		String[] firstLine = lineSplit[0].split(" ");
		String timestamp = firstLine[4];
		Date revisionDate = ISO8601Utils.parse(timestamp);
		                                 
		if(revisionDate.after(earlierDate) && revisionDate.before(laterDate)) {
				revisionId = new IntWritable(Integer.parseInt((firstLine[2])));     
				context.write(key, revisionId);
		}
	}
}
