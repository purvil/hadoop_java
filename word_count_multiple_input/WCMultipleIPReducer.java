package org.hadoop.trainings;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WCMultipleIPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException
	{
		int sum = 0;
		for (IntWritable count : values)
		{
			sum = sum + count.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
