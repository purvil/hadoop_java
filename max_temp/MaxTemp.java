package org.hadoop.trainings;

import java.io.IOException;

// Writable imports
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// Mapper, Reducer import
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//MR application import
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaxTemp {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage:MaxTemp <input path> <output path>");
			System.exit(-1);
		}
		
		
		Job job = new Job();
		
		job.setJarByClass(MaxTemp.class);
		job.setJobName("Max temperature");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaxTempMapper.class);
		job.setCombinerClass(MaxTempReducer.class);
		job.setReducerClass(MaxTempReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}


class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;
	
	
	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		String line = val.toString();
		String year = line.substring(15,19);
		
		int airTemp;
		if (line.charAt(87) == '+') {
			airTemp = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemp = Integer.parseInt(line.substring(87, 92));
		}
		
		String quality = line.substring(92,93);
		
		if (airTemp != MISSING && quality.matches("[01459]")) {
			context.write(new Text(year), new IntWritable(airTemp));
		}
	}
}


class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> val, Context context) throws IOException, InterruptedException{
		int maxVal = Integer.MIN_VALUE;
		for (IntWritable v: val) {
			maxVal = Math.max(maxVal, v.get());
		}
		context.write(key,new IntWritable(maxVal));
	}
}

