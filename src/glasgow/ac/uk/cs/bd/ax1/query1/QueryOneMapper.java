package glasgow.ac.uk.cs.bd.ax1.query1;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

public class QueryOneMapper extends Mapper<LongWritable, Text, Text, Text>
{
	private String earlierTimestamp, laterTimestamp;
	
	@Override 
	public void setup(Context context) 
	{
		Configuration conf = context.getConfiguration();
		//parameters
		earlierTimestamp = conf.get("earlierTimestamp");
		laterTimestamp = conf.get("laterTimestamp");
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String[] lineSplit = line.split("\n");
		String[] firstLine = lineSplit[0].split(" ");
		StringTokenizer st = new StringTokenizer(lineSplit[0]);
		
		System.out.println(lineSplit[0]);
		
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
		
		Date earlier = ISO8601Utils.parse(earlierTimestamp);
		Date later = ISO8601Utils.parse(laterTimestamp);
		                                 
		if(revisionDate.after(earlier) && revisionDate.before(later)) {
				Text MapKeyWord = new Text(firstLine[1]);	      
				Text MapValue = new Text(firstLine[2]);       
				context.write(MapKeyWord, MapValue);
		}
	}
}
