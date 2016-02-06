package query1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer class for query one
 * With input as <articleId, revisionId[]> and output as <articleId, outputString>
 */
public class QueryOneReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
	
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		List<Long> revisionIds = new ArrayList<Long>();
		for (LongWritable value: values){
			revisionIds.add(value.get());
		}
		Collections.sort(revisionIds);
		
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < revisionIds.size(); i++) {
			if (i == revisionIds.size()) {
				sb.append(revisionIds.get(i));
			} else {
				sb.append(revisionIds.get(i)).append(" ");
			}
		}
		
		context.write(key, new Text(revisionIds.size() + " " + sb.toString()));
	}  
}  
