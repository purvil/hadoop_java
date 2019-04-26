package org.hadoop.trainings;

import java.io.IOException;

import java.util.HashMap;
import java.io.FileReader;
import java.io.BufferedReader;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;;

public class SalaryMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	private HashMap<String, Double> desg_map = new HashMap<String, Double>();
	
	public void setup(Context context) throws IOException, InterruptedException {
		BufferedReader br = null;
		Path[] LocalfilesPath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		String record = "";
		
		for (Path path : LocalfilesPath)
		{
		    if (path.getName().toString().trim().equals("distributed_input.txt")) 
		    {
			    br = new BufferedReader(new FileReader(path.toString()));
			    record = br.readLine();          
			    while (record != null) 
			    {
			    	String data[] = record.split(",");      
			    	desg_map.put(data[0].trim(), Double.parseDouble(data[1].trim()));
			    	record = br.readLine();
			    }		
			 }  
		  } 
		}
	
	public void map(LongWritable key, Text value,  Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(",");
		String designation = words[2];
		double n = 1;
		
		if (designation.toString().equalsIgnoreCase("manager")) {
			n = desg_map.get("MGR");
		}
		else if (designation.toString().equalsIgnoreCase("developer")) {
			n = desg_map.get("DLP");
		} 
		else if (designation.toString().equalsIgnoreCase("hr")) {
			n = desg_map.get("HR");
		} else {
			System.err.println("Invalid designation");
		}
		
		int currentSalary = Integer.parseInt(words[3].trim());
	    double increment = (n/100) * currentSalary;
	    
	    context.write(new Text(designation), new DoubleWritable(increment));
		
	}
}
