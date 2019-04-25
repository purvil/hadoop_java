package org.hadoop.trainings;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Mapper;

public class MapperEvenOdd extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] data = value.toString().split(",");
		
		for (String num: data) {
			int number = Integer.parseInt(num);
			
			if ((number % 2) == 1)
				context.write(new Text("ODD"), new IntWritable(number));
			else
				context.write(new Text("EVEN"), new IntWritable(number));
		}
	}
}
