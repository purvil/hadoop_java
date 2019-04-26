package org.hadoop.trainings;


import java.util.*;
import java.util.Date;
import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class FraudReducer extends Reducer<Text, FraudWritable, Text, IntWritable>
{
    ArrayList<String> customers = new ArrayList<String>();
    
    protected void reduce(Text key, Iterable<FraudWritable> values, Context c)	throws IOException, java.lang.InterruptedException
    {
	int fraudPoints = 0;
	int returnsCount = 0;
	int ordersCount = 0;

	FraudWritable data = null;        
	Iterator<FraudWritable> valuesIter = values.iterator();

	while (valuesIter.hasNext())
	{
	    ordersCount++;                      

	    data = valuesIter.next();     

	    if (data.getReturned())
	    {
	    	returnsCount++;                      
	    	try
	    	{
	    		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
	    		Date receiveDate = sdf.parse(data.getReceiveDate());
	    		Date returnDate = sdf.parse(data.getReturnDate());
		    
	    		long diffInMillies = Math.abs(returnDate.getTime() - receiveDate.getTime());
	    		long diffDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);     
		    
	    		if (diffDays > 10)
	    			fraudPoints++;            
	    	}
	    	catch(Exception e)
	    	{
	    		e.printStackTrace();
	    	}
	    }
	}
	double returnRate = (returnsCount/(ordersCount*1.0))*100;
	if (returnRate >= 50)
	    fraudPoints += 10;

	customers.add(key.toString() + "," + data.getCustomerName() + "," + fraudPoints);
    }

    protected void cleanup(Context c)throws IOException, java.lang.InterruptedException
    {
    	Collections.sort(customers, new Comparator<String>()
			{
    		public int compare(String s1, String s2)
    		{
    			int fp1 = Integer.parseInt(s1.split(",")[2]);
    			int fp2 = Integer.parseInt(s2.split(",")[2]);
		    
    			return -(fp1-fp2);   
    		}});
	for (String f: customers)
	{
	    String[] words = f.split(",");
	    c.write(new Text(words[0] + "," + words[1]), new IntWritable(Integer.parseInt(words[2])));
	}                   
    }
}
