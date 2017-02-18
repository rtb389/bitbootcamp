package converter;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideReducerRight extends Reducer<CustomKeyClass,Text,NullWritable,Text>{
	@Override
	public void reduce(CustomKeyClass ckc,Iterable<Text> values, Context context) throws IOException,InterruptedException{
		int i = 0;
		String pid = ckc.getPrimaryId().toString();
		String output = " userId="+ pid;

		NullWritable nullWritableKey = null;
		Text outputValue = new Text("");
		
		if (pid.equals("-5")){

		}else if (pid.equals("-10")){
			for (Text v:values){
				String s = v.toString();
				outputValue.set(s+"userId=N/A DisplayName=N/A>");
				context.write(nullWritableKey, outputValue);
			}
		}else{
		
			for(Text v:values){
				if (i==0){
					String s = v.toString();
					if (s.substring(0, 1).equals("1")){
						output = output + " DisplayName="+s.substring(1)+">";
					}else{
						output = output + " DisplayName=N/A>";
						outputValue.set(v.toString()+output);
						context.write(nullWritableKey, outputValue);
					}
					i=1;
				}else{
					i=2;
					outputValue.set(output+v.toString());
					context.write(nullWritableKey, outputValue);
				}				
			}
			
		}
	}
}