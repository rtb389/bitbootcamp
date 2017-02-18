package reduceside;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TableTwoMapper extends Mapper<LongWritable,Text,CustomKeyClass,Text>{
	@Override
	public void map(LongWritable key,Text value,Context context)
	throws IOException, InterruptedException{
		String[] recordFields = value.toString().split("\t");
		IntWritable primaryId = new IntWritable(Integer.parseInt(recordFields[0]));
		Text name = new Text(", user_name="+recordFields[1]);
		IntWritable tableTag = new IntWritable(1);
		
		CustomKeyClass ckc = new CustomKeyClass(primaryId,tableTag);
		
		context.write(ckc, name);
	}
}
