package query1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utility.Properties;
import utility.WikiModificationFileInputFormat;

public class QueryOneDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("QueryOneDriver");
		job.setJarByClass(QueryOneDriver.class);
		job.setMapperClass(QueryOneMapper.class);
		job.setReducerClass(QueryOneReducer.class);
		job.setInputFormatClass(WikiModificationFileInputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		WikiModificationFileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.getConfiguration().set("earlierTimestamp", args[2]);
		job.getConfiguration().set("laterTimestamp", args[3]);
			

		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF));
		conf.set("mapreduce.job.jar", Properties.PATH_TO_JAR);
		System.exit(ToolRunner.run(conf, new QueryOneDriver(), args));
	}
}
