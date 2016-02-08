package query3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ArticleIDTimestampWritable implements Writable, WritableComparable<ArticleIDTimestampWritable> {
	
	private Long articleId;
	private String timeStamp;
	
	public ArticleIDTimestampWritable() {
	}

	public ArticleIDTimestampWritable(long articleId, String timeStamp) {
		this.articleId = articleId;
		this.timeStamp = timeStamp;
	}

	public void readFields(DataInput dataInput) throws IOException {
		articleId = WritableUtils.readVLong(dataInput);
		timeStamp = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVLong(dataOutput, articleId);
		WritableUtils.writeString(dataOutput, timeStamp);
	}

	public Long getArticleId() {
		return articleId;
	}

	public void setArticleId(Long articleId) {
		this.articleId = articleId;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String toString() {
		return (new StringBuilder().append(articleId)).append(" ").append(timeStamp).toString();
	}

	@Override
	public int compareTo(ArticleIDTimestampWritable cmpObj) {
		return articleId.compareTo(cmpObj.getArticleId());
	}
}