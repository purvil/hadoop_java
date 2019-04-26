package org.hadoop.trainings;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import java.net.URISyntaxException;


public class SalaryDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Job job = new Job();
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
		
		job.setJarByClass(SalaryDriver.class);
		job.setMapperClass(SalaryMapper.class);
		job.setReducerClass(SalaryReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
