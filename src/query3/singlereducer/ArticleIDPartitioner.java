package query3.singlereducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner to ensure all key value pairs with the same key go to the same reducer 
 * 
 * @author vincentfung13
 */
public class ArticleIDPartitioner extends Partitioner<ArticleIDTimestampWritable, LongWritable> {

	@Override
	public int getPartition(ArticleIDTimestampWritable key, LongWritable value, int numPartitions) {
		int hash = key.getArticleId().hashCode();
        return hash % numPartitions;
	}

}
