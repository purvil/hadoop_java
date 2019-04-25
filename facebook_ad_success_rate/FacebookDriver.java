package org.hadoop.trainings;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Writable types
import org.apache.hadoop.io.Text;

public class FacebookDriver {

	public static void main(String[] args) throws Exception{
		if (args.length != 2)
			System.err.println("Invalid usage: FacebookDriver <input> <output>");
		
		Job job = new Job();
		
		job.setJarByClass(FacebookDriver.class);
		job.setJobName("Facebook advertisement success rate");
		
		job.setMapperClass(FacebookMapper.class);
		job.setReducerClass(FacebookReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}

}
