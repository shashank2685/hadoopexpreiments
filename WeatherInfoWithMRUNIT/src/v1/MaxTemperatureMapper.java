package v1;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class MaxTemperatureMapper 
extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable Key, Text value, Context context) 
			throws IOException, InterruptedException{
		String line = value.toString();
		String year = line.substring(15,19);
		String temp = line.substring(87,92);
		if (!missing(temp)) {
			int airTemperature = Integer.parseInt(line.substring(87,92));
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}
	
	private boolean missing(String temp) {
		return temp.equals("+9999");
	}
}