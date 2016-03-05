package hbase.query1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import utility.Properties;

/**
 * Custom partitioner for rowkey
 * The rowkeys in the table are sorted by article id in general, 
 * the partitioner makes use of this, dynamically fetches the first and last rowkeys of the table
 * and get partition accordingly, achieving global ordering of keys.
 * 
 * @author vincentfung13
 */

public class HBaseRowKeyPartitioner extends Partitioner<LongWritable, LongWritable> {
	
	private static Long firstArticleId = null;
	private static Long lastArticleId = null;

	@Override
	public int getPartition(LongWritable key, LongWritable value,
			int numberOfPartition) {
		try {
			fillFirstAndLastID();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		long intervalLength = (lastArticleId - firstArticleId) / numberOfPartition;
		return (int) (key.get() / intervalLength);
	}
	
	private static void fillFirstAndLastID() throws IOException {
		if (firstArticleId == null && lastArticleId == null) {
			// Find first and last key and fill it in
			Configuration conf = HBaseConfiguration.create(new Configuration()); 
			conf.addResource(new Path(Properties.PATH_TO_CORESITE_CONF_HBASE));
			
			Connection connection = ConnectionFactory.createConnection(conf);
			HTable table = (HTable) connection.getTable(TableName.valueOf(Properties.HBASE_TABLE_NAME));
			
			FilterList allFilters = new FilterList(Operator.MUST_PASS_ALL);
			allFilters.addFilter(new KeyOnlyFilter(true));
			allFilters.addFilter(new FirstKeyOnlyFilter());
			
			// Initialize the scan object
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(Properties.HBASE_COLUNMN_FAMILY));
			scan.setFilter(allFilters);
			scan.setMaxResultSize(1L);
			
			// Get first row key
			ResultScanner resultScanner = table.getScanner(scan);
			firstArticleId = Bytes.toLong(resultScanner.next().getRow(), 0);
			
			// Get last row key
			scan.setReversed(true);
			resultScanner = table.getScanner(scan);
			lastArticleId = Bytes.toLong(resultScanner.next().getRow(), 0);
			
			table.close();
			connection.close();
		}
	}
}
