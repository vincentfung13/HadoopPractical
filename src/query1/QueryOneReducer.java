package query1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QueryOneReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for(@SuppressWarnings("unused") Text value: values) {
			count++;
		}
			
		String ReducerValues = Integer.toString(count) + " ";
			
		for(Text value : values){
			ReducerValues = ReducerValues + value.toString() + " ";
		}
			
		Text results = new Text(ReducerValues);
		context.write(key, results);   
	}  
}  
