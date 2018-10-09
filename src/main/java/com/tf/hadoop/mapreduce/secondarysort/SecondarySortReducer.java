package com.tf.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {
    @Override
    /**
     * @param key是一个DateTemperaturePair对象
     * @param value是一个温度列表
     */
    protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values){
            builder.append(value.toString());
            builder.append(",");
        }
        context.write(key.getYearMonth(),new Text(builder.toString()));
    }
}
