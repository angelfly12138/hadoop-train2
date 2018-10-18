package com.tf.hadoop.mapreduce.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private int topN;
	private String delimiter;
	private int keyIndex;
	private int valueIndex;
	private final int DEFAULT_TOP_N = 5;
	private final String DEFAULT_DELIMITER = ",";
	private long startTime;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = context.getConfiguration();
		topN = conf.getInt("topN", DEFAULT_TOP_N);
		delimiter = conf.get("delimiter", DEFAULT_DELIMITER);
		keyIndex = conf.getInt("key_index", 0);
		valueIndex = conf.getInt("value_index", 1);
		startTime = System.currentTimeMillis();
	}
	
	private Map<String, Integer> topMap = new TreeMap<String, Integer>();
	int minValue = Integer.MAX_VALUE;
	String keyOfMinValue = "Not Found";
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String[] items = value.toString().split(delimiter);
		
		String outputKey = items[keyIndex];
		Integer outputValue = Integer.valueOf(items[valueIndex]);
		
		if(outputValue <= minValue) {
			keyOfMinValue = outputKey;
			minValue = outputValue;
		}
		topMap.put(outputKey, outputValue);
		
		if(topMap.size() > topN) {
			topMap.remove(keyOfMinValue);
		} 
	}
	

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(String key : topMap.keySet()) {
			context.write(new Text(key), new IntWritable(topMap.get(key)));
		}
	}


	
}
