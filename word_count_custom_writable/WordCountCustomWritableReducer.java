package org.hadoop.trainings;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCustomWritableReducer extends Reducer<WCWritable, IntWritable, WCWritable, IntWritable> {
	public void reduce(WCWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}