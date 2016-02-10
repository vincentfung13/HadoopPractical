package query3.singlereducer;

import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Comparator used in the sorting phase.
 * 
 * @author vincentfung13
 */
public class CompositeKeyComparator extends WritableComparator{
	
	public CompositeKeyComparator() {
		super(ArticleIDTimestampWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ArticleIDTimestampWritable firstComposite = (ArticleIDTimestampWritable) w1;
		ArticleIDTimestampWritable secondComposite = (ArticleIDTimestampWritable) w2;
		
		int result = firstComposite.getArticleId().compareTo(secondComposite.getArticleId());
		if (result == 0) {
			Date cmpDate = ISO8601Utils.parse(secondComposite.getTimeStamp());
			Date originDate = ISO8601Utils.parse(firstComposite.getTimeStamp());
			result = -originDate.compareTo(cmpDate);
		}
		
		return result;
	}
}
