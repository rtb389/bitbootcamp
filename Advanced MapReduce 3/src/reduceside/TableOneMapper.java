package reduceside;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TableOneMapper extends Mapper<LongWritable,Text,CustomKeyClass,Text>{
	@Override
	public void map(LongWritable key,Text value,Context context)
	throws IOException, InterruptedException{
		String[] recordFields = value.toString().split("\t");
		IntWritable primaryId = new IntWritable(Integer.parseInt(recordFields[1]));
		Text post = new Text(", comments="+recordFields[2]+", post_shared="+recordFields[3]+"]");
		IntWritable tableTag = new IntWritable(2);
		
		CustomKeyClass ckc = new CustomKeyClass(primaryId,tableTag);
		
		context.write(ckc, post);
	}
}
