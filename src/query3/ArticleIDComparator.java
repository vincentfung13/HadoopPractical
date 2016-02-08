package query3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ArticleIDComparator extends WritableComparator{
	
	public ArticleIDComparator() {
		super(ArticleIDTimestampWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ArticleIDTimestampWritable firstComposite = (ArticleIDTimestampWritable) w1;
		ArticleIDTimestampWritable secondComposite = (ArticleIDTimestampWritable) w2;
		
		int result = firstComposite.compareTo(secondComposite);
		
		return result;
	}
}
