package hbase.query2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;

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
public class HBaseQueryTwoDriver extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJobName("HBaseQueryTwoDriver");
		job.setJarByClass(HBaseQueryTwoDriver.class);
		
		job.setCombinerClass(HBaseQueryTwoCombiner.class);
		job.setReducerClass(HBaseQueryTwoReducer.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		// Initialize the scan object
		long earlierTimestampLong = ISO8601Utils.parse(args[1]).getTime();
		long laterTimestampLong = ISO8601Utils.parse(args[2]).getTime();
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(Properties.HBASE_COLUNMN_FAMILY));
		scan.setFilter(new KeyOnlyFilter(true));
		scan.setTimeRange(earlierTimestampLong, laterTimestampLong);
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		job.getConfiguration().set("k", args[3]);
		
		// Initialize table mapper job
		TableMapReduceUtil.initTableMapperJob(Properties.HBASE_TABLE_NAME, 
				scan, HBaseQueryTwoMapper.class, LongWritable.class, IntWritable.class, job); 
			
		job.submit();
		return (job.waitForCompletion(true)? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF_HBASE));
		conf.set("mapreduce.job.jar", Properties.PATH_TO_JAR);
		ToolRunner.run(conf, new HBaseQueryTwoDriver(), args);
		
		System.out.println("INFO: Mapreduce job finsihed, printing out the results:");
		try {
			FileSystem fs = FileSystem.get(conf);
			Path jobOutputPath = new Path(args[0]);
			int k = Integer.parseInt(args[3]);
		
			TreeMap<Integer, PriorityQueue<Long>> modificationCountToArticle = new TreeMap<Integer, PriorityQueue<Long>>();
			PriorityQueue<Long> articleIdQueue;
			long articleId;
			int modificationCount;
			String[] lineSplit;
			FileStatus[] status = fs.listStatus(jobOutputPath);
			for (int i = 0; i < status.length; i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line;
                line = br.readLine();
                while (line != null) {
                	lineSplit = line.split("\\s+");
                	articleId = Long.parseLong(lineSplit[0]);
                	modificationCount = Integer.parseInt(lineSplit[1]);
                    
                	if (modificationCountToArticle.size() < k) {
            			if (modificationCountToArticle.containsKey(modificationCount)) {
            				articleIdQueue = modificationCountToArticle.get(modificationCount);			
            			}
            			else {
            				articleIdQueue = new PriorityQueue<Long>();
            			}
            			articleIdQueue.add(articleId);
            			modificationCountToArticle.put(modificationCount, articleIdQueue);
            		}
            		else if (modificationCountToArticle.size() == k) {
            			if (modificationCountToArticle.firstKey() < modificationCount) {
            				if (modificationCountToArticle.containsKey(modificationCount)) {
            					articleIdQueue = modificationCountToArticle.get(modificationCount);			
            				}
            				else {
            					articleIdQueue = new PriorityQueue<Long>();
            				}
            				articleIdQueue.add(articleId);
            				modificationCountToArticle.remove(modificationCountToArticle.firstKey());
            				modificationCountToArticle.put(modificationCount, articleIdQueue);
            			}
            		}
                	
                	line = br.readLine();
                }
			}
			
			Iterator<Integer> itr = modificationCountToArticle.descendingKeySet().iterator();
    		while (itr.hasNext()) {
    			int key = itr.next();
    			PriorityQueue<Long> articleIdQueueItr = modificationCountToArticle.get(key);
    			while (articleIdQueueItr.size() > 0) {
    				System.out.println(articleIdQueueItr.poll() + " " + key);
    			}
    		}
			
		} catch (FileNotFoundException e) {
			System.err.println("ERROR: File not found.");
		}	
	}
}
