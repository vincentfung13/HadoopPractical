package mapreduce.query3.secondarysorting;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utility.Properties;
import utility.WikiModificationCompositeKeyInputFormat;
import utility.WikiModificationFileInputFormat;

/**
 * Driver class of the single reducer solution of query 3
 * 
 * @author vincentfung13
 */
public class QueryThreeSecondarySortingDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("QueryThreeSecondingSortingDriver");
		job.setJarByClass(QueryThreeSecondarySortingDriver.class);
		job.setNumReduceTasks(Properties.NUM_REDUCER_TASK);
		
		job.setMapperClass(QueryThreeSecondarySortingMapper.class);
		job.setReducerClass(QueryThreeSecondarySortingReducer.class);
		
		job.setInputFormatClass(WikiModificationCompositeKeyInputFormat.class);
		job.setMapOutputKeyClass(ArticleIDTimestampWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(ArticleIDTimestampWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setGroupingComparatorClass(ArticleIDGroupingComparator.class);
		job.setPartitionerClass(ArticleIDPartitioner.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		Path partitionFile = new Path(Properties.PARTITIONING_PATH_COMPOSITE_KEY);
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
		
		// Taking key samples from the input file if the partition file doesn't exist
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] status = fs.listStatus(partitionFile);
		if (status.length > 0) {
			double pcnt = 10.0;
			int numSamples = Properties.NUM_REDUCER_TASK;
			int maxSplits = Properties.NUM_REDUCER_TASK - 1;
			if (0 >= maxSplits)
					maxSplits = Integer.MAX_VALUE;
			InputSampler.Sampler<IntWritable, Text> sampler = 
					new InputSampler.RandomSampler<IntWritable, Text>(pcnt, numSamples, maxSplits);
			InputSampler.writePartitionFile(job, sampler);
		}
		
		// Set input path and output path
		WikiModificationFileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.getConfiguration().set("timestamp", args[2]);
		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF_MAPREDUCE));
		conf.set("mapreduce.job.jar", Properties.PATH_TO_JAR);
		ToolRunner.run(conf, new QueryThreeSecondarySortingDriver(), args);
		
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
