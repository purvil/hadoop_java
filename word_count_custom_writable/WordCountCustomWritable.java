package org.hadoop.trainings;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountCustomWritable {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(WordCountCustomWritable.class);
		job.setJobName("Word count");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setMapperClass(WordCountCustomWritableMapper.class);
		job.setCombinerClass(WordCountCustomWritableReducer.class);
		job.setReducerClass(WordCountCustomWritableReducer.class);
		
		
		job.setOutputKeyClass(WCWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
