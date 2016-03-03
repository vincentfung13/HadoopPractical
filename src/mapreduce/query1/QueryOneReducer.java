package mapreduce.query1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for query one
 * With input as <articleId, revisionId[]> and output as <articleId, outputString>
 * 
 * @author vincentfung13
 */
public class QueryOneReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		List<Integer> revisionIds = new ArrayList<Integer>();
		for (IntWritable value: values){
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
