package utility;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ModificationCountPartitioner extends Partitioner<LongWritable, LongWritable>{

	@Override
	public int getPartition(LongWritable key, LongWritable value, int numPartitions) {
		return (int) key.get();
	}

}
