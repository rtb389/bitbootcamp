package reduceside;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideReducer extends Reducer<CustomKeyClass,Text,NullWritable,Text>{
	@Override
	public void reduce(CustomKeyClass ckc,Iterable<Text> values, Context context) throws IOException,InterruptedException{
		int i = 0;
		String output = "ActivityVO [userId="+ ckc.getPrimaryId().toString();
		
		NullWritable nullWritableKey = null;
		Text outputValue = new Text("");
		
		for(Text v:values){
			if (i==0){
				output = output + v.toString();
				i=1;
			}else{
				outputValue.set(output+v.toString());
				context.write(nullWritableKey, outputValue);
			}				
		}	
	}
}
