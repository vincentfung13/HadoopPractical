package query3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utility.WikiModificationFileInputFormat;

public class QueryThreeDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("QueryThreeDriver");
		job.setJarByClass(QueryThreeDriver.class);
		
		job.setMapperClass(QueryThreeMapper.class);
		job.setReducerClass(QueryThreeReducer.class);
		
		job.setInputFormatClass(WikiModificationFileInputFormat.class);
		job.setMapOutputKeyClass(ArticleIDTimestampWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(ArticleIDTimestampWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setSortComparatorClass(ArticleIDComparator.class);
		job.setGroupingComparatorClass(TimestampComparator.class);
		job.setPartitionerClass(ArticleIDPartitioner.class);
		
		WikiModificationFileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.getConfiguration().set("timestamp", args[2]);
		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/users/level4/2104275f/BD4/bd4-hadoop/conf/core-site.xml"));
		conf.set("mapreduce.job.jar", "file:///users/level4/2104275f/BD4/QueryOneDriver.jar");
		System.exit(ToolRunner.run(conf, new QueryThreeDriver(), args));
	}
}
