package org.hadoop.trainings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountCustomWritableMapper extends Mapper<LongWritable, Text, WCWritable, IntWritable> {
	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		String line = val.toString();
		String[] words = line.split(",");
		
		for(String word: words) {
			context.write(new WCWritable(word.toUpperCase().trim()), new IntWritable(1));
		}
	}
}