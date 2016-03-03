package mapreduce.query3.secondarysorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

/**
 * Composite key for secondary sorting
 * 
 * @author vincentfung13
 */
public class ArticleIDTimestampWritable implements Writable, WritableComparable<ArticleIDTimestampWritable> {
	
	// This is the natural key
	private Integer articleId;
	private String timeStamp;
	
	public ArticleIDTimestampWritable() {
	}

	public ArticleIDTimestampWritable(int articleId, String timeStamp) {
		this.articleId = articleId;
		this.timeStamp = timeStamp;
	}

	public void readFields(DataInput dataInput) throws IOException {
		articleId = WritableUtils.readVInt(dataInput);
		timeStamp = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVLong(dataOutput, articleId);
		WritableUtils.writeString(dataOutput, timeStamp);
	}

	public Integer getArticleId() {
		return articleId;
	}

	public void setArticleId(int articleId) {
		this.articleId = articleId;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String toString() {
		return articleId.toString();
	}

	@Override
	public int compareTo(ArticleIDTimestampWritable cmpObj) {
		int result = articleId.compareTo(cmpObj.getArticleId());
		if (result == 0) {
			Date cmpDate = ISO8601Utils.parse(cmpObj.timeStamp);
			Date originDate = ISO8601Utils.parse(timeStamp);
			result = -originDate.compareTo(cmpDate);
		}
		return result;
	}
}