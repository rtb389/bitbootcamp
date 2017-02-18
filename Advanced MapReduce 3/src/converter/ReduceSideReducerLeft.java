package converter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideReducerLeft extends Reducer<CustomKeyClass,Text,NullWritable,Text>{
	@Override
	public void reduce(CustomKeyClass ckc,Iterable<Text> values, Context context) throws IOException,InterruptedException{
		int i = 0;
		String pid = ckc.getPrimaryId().toString();
		String output = " userId="+ pid;

		NullWritable nullWritableKey = null;
		Text outputValue = new Text("");
		
		if (pid.equals("-5")){
			for (Text v:values){
				String s = v.toString();
				outputValue.set("<Id=N/A PostId=N/A Score=N/A Text=N/A CreationDate=N/A userId=N/A DisplayName="+s.substring(1)+">");
				context.write(nullWritableKey, outputValue);
			}
		}else if (pid.equals("-10")){
			
		}else{
		
			for(Text v:values){
				if (i==0){
					String s = v.toString();
					if (s.substring(0, 1).equals("1")){
						output = output + " DisplayName="+s.substring(1)+">";
					}else{
						break;
					}
					i=1;
				}else{
					i=2;
					outputValue.set(v.toString()+output);
					context.write(nullWritableKey, outputValue);
				}				
			}
			//If there was nothing in the right
			if (i==1){
				outputValue.set("<Id=N/A PostId=N/A Score=N/A Text=N/A CreationDate=N/A"+output);
				context.write(nullWritableKey,outputValue);
			}
		}
	}
}