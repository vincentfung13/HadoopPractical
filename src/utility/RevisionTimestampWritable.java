package utility;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class RevisionTimestampWritable implements Writable,
	WritableComparable<RevisionTimestampWritable> {

	private Long revisionId;
	private String timeStamp;

	public RevisionTimestampWritable(long revisionId, String timeStamp) {
		this.revisionId = revisionId;
		this.timeStamp = timeStamp;
	}

	public void readFields(DataInput dataInput) throws IOException {
		revisionId = WritableUtils.readVLong(dataInput);
		timeStamp = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVLong(dataOutput, revisionId);
		WritableUtils.writeString(dataOutput, timeStamp);
	}
	
	@Override
	public int compareTo(RevisionTimestampWritable obj) {
		return revisionId.compareTo(obj.revisionId);
	}
	
	public Long getRevisionId() {
		return revisionId;
	}

	public void setRevisionId(Long revisionId) {
		this.revisionId = revisionId;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String toString() {
		return (new StringBuilder().append(revisionId)).append(" ").append(timeStamp).toString();
	}
}