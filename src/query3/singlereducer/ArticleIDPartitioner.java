package query3.singlereducer;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ArticleIDPartitioner extends Partitioner<ArticleIDTimestampWritable, LongWritable> {

	@Override
	public int getPartition(ArticleIDTimestampWritable key, LongWritable value, int numPartitions) {
		int hash = key.getArticleId().hashCode();
        return hash % numPartitions;
	}

}
