package converter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CustomKeyClass implements Writable,WritableComparable<CustomKeyClass>{

	private IntWritable primaryId = new IntWritable();
	private IntWritable tableTag = new IntWritable();
	
	public CustomKeyClass(){
		
	}
	
	public CustomKeyClass(IntWritable primaryId,IntWritable tableTag){
		super();
		this.primaryId = primaryId;
		this.tableTag = tableTag;
	}
	
	public IntWritable getPrimaryId(){
		return primaryId;
	}
	
	public void setPrimaryId(IntWritable primaryId){
		this.primaryId = primaryId;
	}
	
	public IntWritable getTableTag(){
		return tableTag;
	}
	
	public void setTableTag(IntWritable tableTag){
		this.tableTag=tableTag;
	}
	
	@Override
	public int compareTo(CustomKeyClass o) {
		int compareValue = this.primaryId.compareTo(o.getPrimaryId());
		if (compareValue == 0){
			compareValue = this.tableTag.compareTo(o.getTableTag());
		}
		return compareValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		primaryId.readFields(in);
		tableTag.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		primaryId.write(out);
		tableTag.write(out);
	}
	
}
