package org.hadoop.trainings;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMultipleIPMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	{
		String[] data = value.toString().split(" ");
		for(String temp:data) {
			context.write(new Text(temp), new IntWritable(1));
		}
	}
}
