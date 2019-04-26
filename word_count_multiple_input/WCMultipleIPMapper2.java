package org.hadoop.trainings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMultipleIPMapper2 extends Mapper<Text, Text, Text, IntWritable> {
	public void map(Text key, Text value, Context context)throws IOException, InterruptedException
	{
		context.write(value, new IntWritable(1));
	}
}
