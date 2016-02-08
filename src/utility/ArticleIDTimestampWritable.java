package utility;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.htrace.fasterxml.jackson.databind.util.ISO8601Utils;

public class ArticleIDTimestampWritable implements Writable,
	WritableComparable<ArticleIDTimestampWritable> {

	private Long articleId;
	private String timeStamp;

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
	
	@Override
	public int compareTo(ArticleIDTimestampWritable obj) {
		Date thisDate = ISO8601Utils.parse(timeStamp); 
		Date objDate = ISO8601Utils.parse(obj.timeStamp);
		return thisDate.compareTo(objDate);
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
}