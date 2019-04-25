package org.hadoop.trainings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FacebookReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		HashMap<String, String> cityData = new HashMap<String, String>();
		
		Iterator<Text> iter = values.iterator();
		
		while (iter.hasNext()) {
			String data = iter.next().toString();
			
			String[] words = data.split(",");
			
			String location = words[0].trim();
			int totalClick = Integer.parseInt(words[1].trim());
			int convertedClick = Integer.parseInt(words[2].trim());
			
			double succRate = (convertedClick / (totalClick*1.0)) * 100;
			
			if (cityData.containsKey(location)) {
				String val = cityData.get(location);
				String[] nums = val.split(",");
				succRate += Double.parseDouble(nums[0]);
				int count = Integer.parseInt(nums[1]) + 1;
				cityData.put(location, Double.toString(succRate) + "," + Integer.toString(count));
			} else {
				cityData.put(location, Double.toString(succRate) + ",1");
			}
		}
		
		for (Map.Entry<String, String> e:cityData.entrySet()) {
			String[] nums = e.getValue().split(",");
			double avg = Double.parseDouble(nums[0]) / Integer.parseInt(nums[1]);
			context.write(key, new Text(e.getKey() + "," + Double.toString(avg)));
		}
		
	}
}
