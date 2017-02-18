package reduceside;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReduceSideJoinGroupingComparator extends WritableComparator{
	public ReduceSideJoinGroupingComparator(){
		super(CustomKeyClass.class,true);
	}
	
	@Override
	public int compare(WritableComparable obj1,WritableComparable obj2){
		CustomKeyClass o1 = (CustomKeyClass) obj1;
		CustomKeyClass o2 = (CustomKeyClass) obj2;
		
		return o1.getPrimaryId().compareTo(o2.getPrimaryId());
	}

}
