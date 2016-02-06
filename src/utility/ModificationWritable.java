package utility;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ModificationWritable implements Writable,
		WritableComparable<ModificationWritable> {

	private Long articleId;
	private Long modifications;

	public ModificationWritable(long articleId, long modifications) {
		this.articleId = articleId;
		this.modifications = modifications;
	}

	public void readFields(DataInput dataInput) throws IOException {
		articleId = WritableUtils.readVLong(dataInput);
		modifications = WritableUtils.readVLong(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeVLong(dataOutput, articleId);
		WritableUtils.writeVLong(dataOutput, modifications);
	}
	
	@Override
	public int compareTo(ModificationWritable objKeyPair) {
		int result = modifications.compareTo(objKeyPair.modifications);
		if (result == 0) {
			result = articleId.compareTo(objKeyPair.articleId);
		}
		return result;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder().append(articleId)).append(" ")
				.append(modifications).toString();
	}
	
	public Long getArticleId() {
		return articleId;
	}

	public void setArticleId(Long articleId) {
		this.articleId = articleId;
	}

	public Long getModifications() {
		return modifications;
	}

	public void setModifications(Long modifications) {
		this.modifications = modifications;
	}
}
