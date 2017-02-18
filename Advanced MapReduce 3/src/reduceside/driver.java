package reduceside;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class driver {

	public static void main(String[] args) throws IOException,
	ClassNotFoundException,InterruptedException{
		// TODO Auto-generated method stub
		if(args.length != 3){
			System.out.println("The number of arguments should be 3: <input folder 1><input folder 2><output folder>");
			System.out.println("Shutting off .jar please try again with the right number of parameters");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(driver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TableOneMapper.class);
		
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TableTwoMapper.class);
		
		job.setMapOutputKeyClass(CustomKeyClass.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setSortComparatorClass(ReduceSideJoinSortingComparator.class);
		
		job.setGroupingComparatorClass(ReduceSideJoinGroupingComparator.class);
		
		job.setReducerClass(ReduceSideReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
		
	}
}