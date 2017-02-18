package reduceside;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReduceSideJoinSortingComparator extends WritableComparator{
	public ReduceSideJoinSortingComparator(){
		super(CustomKeyClass.class,true);
	}
	
	@Override
	public int compare(WritableComparable obj1,WritableComparable obj2){
		CustomKeyClass o1 = (CustomKeyClass) obj1;
		CustomKeyClass o2 = (CustomKeyClass) obj2;
		
		int cmpResult = o1.getPrimaryId().compareTo(o2.getPrimaryId());
		if(cmpResult==0){
			cmpResult=Double.compare(o1.getTableTag().get(), o2.getTableTag().get());
		}
		return cmpResult;
	}
	
	
	
}
