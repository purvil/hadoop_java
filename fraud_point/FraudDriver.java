package org.hadoop.trainings;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FraudDriver
{
    public static void main(String[] args) throws IOException,  ClassNotFoundException,	 InterruptedException
    {

	Path inputPath = new Path(args[0]);
	Path outputDir = new Path(args[1]);
	
	Job job = new Job();
	

	job.setJarByClass(FraudDriver.class);
	job.setJobName("Fraud Detection");
	
	job.setMapperClass(FraudMapper.class);
	job.setReducerClass(FraudReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FraudWritable.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputDir);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
  	
