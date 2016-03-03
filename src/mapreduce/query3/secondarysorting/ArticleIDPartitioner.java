package mapreduce.query3.secondarysorting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner to ensure all key value pairs with the same key go to the same reducer 
 * 
 * @author vincentfung13
 */
public class ArticleIDPartitioner extends Partitioner<ArticleIDTimestampWritable, IntWritable> {

	@Override
	public int getPartition(ArticleIDTimestampWritable key, IntWritable value, int numPartitions) {
		int hash = key.getArticleId().hashCode();
        return hash % numPartitions;
	}

}
