package query3;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

import utility.ArticleIDTimestampWritable;

public class QueryThreeMapper extends Mapper<LongWritable, Text, LongWritable, ArticleIDTimestampWritable>{

	
	
	@Override
	public void setup(Context context) {
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplit = line.split("\n");
		String[] firstLine = lineSplit[0].split(" ");
		StringTokenizer st = new StringTokenizer(lineSplit[0]);
		
		String revisionId = firstLine[4];
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
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
	}
}
