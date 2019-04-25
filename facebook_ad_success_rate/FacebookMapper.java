package org.hadoop.trainings;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class FacebookMapper extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(",");
		
		context.write(new Text(words[3]), new Text(words[2] + "," + words[4] + "," + words[5]));
	}
}
