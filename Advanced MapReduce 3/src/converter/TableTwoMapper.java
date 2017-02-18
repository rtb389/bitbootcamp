package converter;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TableTwoMapper extends Mapper<LongWritable,Text,CustomKeyClass,Text>{
	@Override
	public void map(LongWritable key,Text value,Context context)
	throws IOException, InterruptedException{
		String notempty = value.toString();
		if(!notempty.equals("")){
			Map<String, String> recordFields = XMLConverter.transformXMLToMap(notempty);
			//Empty exception
			IntWritable primaryId;
			if (!recordFields.containsKey("UserId")){
				primaryId = new IntWritable(-10);
			}else{
				primaryId = new IntWritable(Integer.parseInt(recordFields.get("UserId")));
			}
			String id = recordFields.get("Id");
			if (id == null){
				id = "N/A";
			}
			String postid = recordFields.get("PostId");
			if (postid == null){
				postid = "N/A";
			}
			String score = recordFields.get("Score");
			if (score == null){
				score = "N/A";
			}
			String text = recordFields.get("Text");
			if (text == null){
				text = "N/A";
			}
			String date = recordFields.get("CreationDate");
			if (date == null){
				date = "N/A";
			}
	
			Text name = new Text("<Id="+id+" PostId="+postid+" Score="+score+" Text="+text+" CreationDate="+date);
			IntWritable tableTag = new IntWritable(2);
			
			CustomKeyClass ckc = new CustomKeyClass(primaryId,tableTag);
			
			context.write(ckc, name);
		}
	}
}
