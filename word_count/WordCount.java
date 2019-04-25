package org.hadoop.trainings;

import java.io.IOException;

// For driver class
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// Writable types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// for map and reduce
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCount {
	public static void main(String args[]) throws Exception{
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("Word count");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.setMapperClass(MapperWordCount.class);
		job.setCombinerClass(ReducerWordCount.class);
		job.setReducerClass(ReducerWordCount.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


class MapperWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		String line = val.toString();
		String[] words = line.split(",");
		
		for(String word: words) {
			context.write(new Text(word.toUpperCase().trim()), new IntWritable(1));
		}
	}
}

class ReducerWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}