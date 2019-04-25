package org.hadoop.trainings;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Writable types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class EvenOddSum {
	public static void main(String[] args) throws Exception{
		
		Job job = new Job();
		job.setJarByClass(EvenOddSum.class);
		job.setJobName("Even Odd Sum");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapperEvenOdd.class);
		job.setReducerClass(ReducerEvenOdd.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}

}

