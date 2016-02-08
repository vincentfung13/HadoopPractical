package query3;

import java.util.Date;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

public class TimestampComparator extends WritableComparator{
	
	public TimestampComparator() {
		super(ArticleIDTimestampWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ArticleIDTimestampWritable firstComposite = (ArticleIDTimestampWritable) w1;
		ArticleIDTimestampWritable secondComposite = (ArticleIDTimestampWritable) w2;
		
		Date firstDate = ISO8601Utils.parse(firstComposite.getTimeStamp());
		Date secondDate = ISO8601Utils.parse(secondComposite.getTimeStamp());
		
		return firstDate.compareTo(secondDate);
	}
}
