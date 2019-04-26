package org.hadoop.trainings;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FraudMapper extends Mapper<LongWritable, Text, Text, FraudWritable>
{

    private Text custId = new Text();    
    private FraudWritable data = new FraudWritable(); 

    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

	String[] words = value.toString().split(",");  

	custId.set(words[0]);             
	
	data.set(words[1], words[5], words[6], words[7]);
	
	c.write(custId, data);
  }
}
