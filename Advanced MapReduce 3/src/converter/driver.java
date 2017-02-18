package converter;

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
		if(args.length != 4){
			System.out.println("The number of arguments should be 3: <input folder 1><input folder 2><type of join><output folder>");
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
		
		//Tell which type of join to do
		switch (args[3]){
		case "left":
			job.setReducerClass(ReduceSideReducerLeft.class);
			break;
		case "right":
			job.setReducerClass(ReduceSideReducerRight.class);
			break;
		case "full":
			job.setReducerClass(ReduceSideReducerFull.class);
			break;
		default: //Inner 
			job.setReducerClass(ReduceSideReducerInner.class);
			break;
		}
		
		job.setMapOutputKeyClass(CustomKeyClass.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setSortComparatorClass(ReduceSideJoinSortingComparator.class);
		
		job.setGroupingComparatorClass(ReduceSideJoinGroupingComparator.class);
		
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(4);
		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		job.waitForCompletion(true);
		
	}
}