package com.tf.hadoop.mapreduce.top;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	private final int DEFAULT_TOP_N = 5;
	int topN;	
	
	SortedMap<String, Integer> topMap = new TreeMap<String, Integer>();
	int minValue = Integer.MAX_VALUE;
	String keyOfMinValue = "Not Found";
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		topN = context.getConfiguration().getInt("topN", DEFAULT_TOP_N);
	}
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable value : values) {
			sum += value.get();
		}
		String outputKey = key.toString();
		if(sum < minValue) {
			keyOfMinValue = outputKey;
			minValue = sum;
		}
		topMap.put(outputKey, sum);
		
		if(topMap.size() > topN) {
			topMap.remove(keyOfMinValue);
		} 
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int maxValue = 0;
		String keyOfMaxValue = "Not Found";
		
		for(int i = 0; i < topN; i++) {
			for(String key : topMap.keySet()) {
				int value = topMap.get(key);
				if(maxValue < value) {
					maxValue = value;
					keyOfMaxValue = key;
				}
			}
			context.write(new Text(keyOfMaxValue), new IntWritable(maxValue));
			topMap.remove(keyOfMaxValue);
			maxValue = 0;
		}
	}

	
}
