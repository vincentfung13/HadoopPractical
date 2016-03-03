package mapreduce.query2;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utility.Properties;
import utility.WikiModificationFileInputFormat;

/**
 * Driver class for query two.
 * 
 * @author 2104275f
 */
public class QueryTwoDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("QueryTwoDriver");
		job.setJarByClass(QueryTwoDriver.class);
		
		job.setMapperClass(QueryTwoMapper.class);
		job.setCombinerClass(QueryTwoCombiner.class);
		job.setReducerClass(QueryTwoReducer.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(WikiModificationFileInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		WikiModificationFileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.getConfiguration().set("earlierTimestamp", args[2]);
		job.getConfiguration().set("laterTimestamp", args[3]);
		job.getConfiguration().set("k", args[4]);
			
		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF));
		conf.set("mapreduce.job.jar", Properties.PATH_TO_JAR);
		ToolRunner.run(conf, new QueryTwoDriver(), args);
		
		System.out.println("INFO: Mapreduce job finsihed, printing out the results:");
		try {
			FileSystem fs = FileSystem.get(conf);
			Path jobOutputPath = new Path(args[1]);
		
			FileStatus[] status = fs.listStatus(jobOutputPath);
			for (int i = 0; i < status.length; i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line;
                line = br.readLine();
                while (line != null){
                	System.out.println(line);
                    line = br.readLine();
                }
			}
		} catch (Exception e) {
			System.err.println("ERROR: File not found.");
		}	
	}
}
