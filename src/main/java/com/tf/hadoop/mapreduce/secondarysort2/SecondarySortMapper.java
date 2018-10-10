package com.tf.hadoop.mapreduce.secondarysort2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;

/**
 * 定义map()
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text,CompositeKey, NaturalValue> {

    private final CompositeKey reducerKey = new CompositeKey();
    private final NaturalValue reducerValue = new NaturalValue();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString().trim(),",");
        if (tokens.length == 3){
            // tokens[0] = stokSymbol
            // tokens[1] = timestamp (as date)
            // tokens[2] = price as double
            Date date = DateUtil.getDate(tokens[1]);
            if (date == null){
                return;
            }
            long timestamp = date.getTime();
            reducerKey.set(tokens[0],timestamp);
            reducerValue.set(timestamp, Double.parseDouble(tokens[2]));
            context.write(reducerKey, reducerValue);
        }
        else{
            // 忽略实体类或者日志错误，没有足够的tokens
        }
    }
}
