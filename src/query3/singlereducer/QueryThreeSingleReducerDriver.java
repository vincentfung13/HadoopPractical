package query3.singlereducer;

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

public class QueryThreeSingleReducerDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("QueryThreeSingleReducerDriver");
		job.setJarByClass(QueryThreeSingleReducerDriver.class);
		
		job.setMapperClass(QueryThreeSecondarySortingMapper.class);
		job.setReducerClass(QueryThreeSecondarySortingReducer.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(WikiModificationFileInputFormat.class);
		job.setMapOutputKeyClass(ArticleIDTimestampWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(ArticleIDTimestampWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setGroupingComparatorClass(ArticleIDGroupingComparator.class);
		job.setPartitionerClass(ArticleIDPartitioner.class);
		
		// Set input path and output path
		WikiModificationFileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.getConfiguration().set("timestamp", args[2]);
		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF));
		conf.set("mapreduce.job.jar", Properties.PATH_TO_JAR);
		System.exit(ToolRunner.run(conf, new QueryThreeSingleReducerDriver(), args));
	}
}