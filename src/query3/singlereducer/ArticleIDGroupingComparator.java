package query3.singlereducer;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ArticleIDGroupingComparator extends WritableComparator{
	
	public ArticleIDGroupingComparator() {
		super(ArticleIDTimestampWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ArticleIDTimestampWritable firstComposite = (ArticleIDTimestampWritable) w1;
		ArticleIDTimestampWritable secondComposite = (ArticleIDTimestampWritable) w2;
		
		return firstComposite.getArticleId().compareTo(secondComposite.getArticleId());
	}
}
