package query3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utility.Properties;
import utility.WikiModificationFileInputFormat;

public class QueryThreeDriver extends Configured implements Tool {
	
	private final int NUM_REDUCER_TASK = 64;
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("QueryThreeDriver");
		job.setJarByClass(QueryThreeDriver.class);
		
		job.setMapperClass(QueryThreeMapper.class);
		job.setReducerClass(QueryThreeReducer.class);
		job.setNumReduceTasks(NUM_REDUCER_TASK);
		
		job.setInputFormatClass(WikiModificationFileInputFormat.class);
		job.setMapOutputKeyClass(ArticleIDTimestampWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(ArticleIDTimestampWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setGroupingComparatorClass(ArticleIDGroupingComparator.class);
		
		// Set inputpath and output path
		WikiModificationFileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Configure total order partitioner
//		job.setPartitionerClass(TotalOrderPartitioner.class);
//		Path partitionFile = new Path("/user/2104275f/partitioning");
//		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
//		double pcnt = 10.0;
//        int numSamples = NUM_REDUCER_TASK;
//        int maxSplits = NUM_REDUCER_TASK - 1;
//        if (0 >= maxSplits)
//            maxSplits = Integer.MAX_VALUE;
//        InputSampler.Sampler<ArticleIDTimestampWritable, LongWritable> sampler = 
//        		new InputSampler.RandomSampler<ArticleIDTimestampWritable, LongWritable>(pcnt, numSamples, maxSplits);
//        InputSampler.writePartitionFile(job, sampler);
		
		job.getConfiguration().set("timestamp", args[2]);
		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF));
		conf.set("mapreduce.job.jar", Properties.PATH_TO_JAR);
		System.exit(ToolRunner.run(conf, new QueryThreeDriver(), args));
	}
}
