package org.hadoop.trainings;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SalaryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
{
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, java.lang.InterruptedException
    {

	double totalIncrement = 0;
	int count = 0;
	
	Iterator<DoubleWritable> valuesIter = values.iterator();

	while (valuesIter.hasNext())
	{
	    count++;
	    totalIncrement += valuesIter.next().get();
	}

	double avgInc = totalIncrement/count;
	context.write(key, new DoubleWritable(avgInc));
    }
}