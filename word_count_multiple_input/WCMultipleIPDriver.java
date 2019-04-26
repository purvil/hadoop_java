package org.hadoop.trainings;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WCMultipleIPDriver {

	public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		Job job = new Job(conf, "Multiple input example");
     	job.setJarByClass(WCMultipleIPDriver.class);
		job.setMapperClass(WCMultipleIPMapper1.class);
		job.setMapperClass(WCMultipleIPMapper2.class);
		job.setReducerClass(WCMultipleIPReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, WCMultipleIPMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), KeyValueTextInputFormat.class, WCMultipleIPMapper2.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
