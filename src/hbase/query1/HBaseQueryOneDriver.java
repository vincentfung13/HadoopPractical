
package hbase.query1;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

import utility.Properties;

/**
 * Driver class for hbase query one.
 * 
 * @author vincentfung13
 */

public class HBaseQueryOneDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("HBaseQueryOneDriver");
		job.setJarByClass(HBaseQueryOneDriver.class);
		
		job.setPartitionerClass(HBaseRowKeyPartitioner.class);
		job.setReducerClass(HBaseQueryOneReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		long earlierTimestampLong = ISO8601Utils.parse(args[1]).getTime();
		long laterTimestampLong = ISO8601Utils.parse(args[2]).getTime();
		
		// Configure the filters to use
		FilterList allFilters = new FilterList(Operator.MUST_PASS_ALL);
		allFilters.addFilter(new KeyOnlyFilter(true));
		allFilters.addFilter(new FirstKeyOnlyFilter());
		
		// Initialize the scan object
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(Properties.HBASE_COLUNMN_FAMILY));
		scan.setFilter(allFilters);
		scan.setTimeRange(earlierTimestampLong, laterTimestampLong);
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		
		// Initialize table mapper job
		TableMapReduceUtil.initTableMapperJob(Properties.HBASE_TABLE_NAME, 
				scan, HBaseQueryOneMapper.class, LongWritable.class, LongWritable.class, job); 

		// Submit the job and wait for completion
		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create(new Configuration()); 
		conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF_HBASE));
		conf.set("mapreduce.job.jar", Properties.PATH_TO_JAR);		
		ToolRunner.run(conf, new HBaseQueryOneDriver(), args);
		
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
