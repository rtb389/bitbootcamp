package converter;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TableOneMapper extends Mapper<LongWritable,Text,CustomKeyClass,Text>{
	@Override
	public void map(LongWritable key,Text value,Context context)
	throws IOException, InterruptedException{
		String notempty = value.toString();
		if(!notempty.equals("")){
			Map<String, String> recordFields = XMLConverter.transformXMLToMap(notempty);
			IntWritable primaryId;
			if (!recordFields.containsKey("Id")){
				primaryId = new IntWritable(-10);
			}else{
				primaryId = new IntWritable(Integer.parseInt(recordFields.get("Id")));
			}
			Text name = new Text("1"+recordFields.get("DisplayName"));
			IntWritable tableTag = new IntWritable(1);
			
			CustomKeyClass ckc = new CustomKeyClass(primaryId,tableTag);
			
			context.write(ckc, name);
		}
	}
}
