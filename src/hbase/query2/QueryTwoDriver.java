package hbase.query2;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

import utility.Properties;

/**
 * Driver class for query two.
 * 
 * @author 2104275f
 */
public class QueryTwoDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("HBaseQueryTwoDriver");
		job.setJarByClass(QueryTwoDriver.class);
		
		job.setCombinerClass(QueryTwoCombiner.class);
		job.setReducerClass(QueryTwoReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		// Initialize the scan object
		long earlierTimestampLong = ISO8601Utils.parse(args[1]).getTime();
		long laterTimestampLong = ISO8601Utils.parse(args[2]).getTime();
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("WD"));
		scan.setFilter(new KeyOnlyFilter(true));
		scan.setTimeRange(earlierTimestampLong, laterTimestampLong);
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		job.getConfiguration().set("k", args[3]);
		
		// Initialize table mapper job
		TableMapReduceUtil.initTableMapperJob("BD4Project2Sample", 
				scan, QueryTwoMapper.class, LongWritable.class, IntWritable.class, job); 
			
		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF_HBASE));
		conf.set("mapreduce.job.jar", Properties.PATH_TO_JAR);
		ToolRunner.run(conf, new QueryTwoDriver(), args);
		
		System.out.println("INFO: Mapreduce job finsihed, printing out the results:");
		try {
			FileSystem fs = FileSystem.get(conf);
			Path jobOutputPath = new Path(args[0]);
		
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
