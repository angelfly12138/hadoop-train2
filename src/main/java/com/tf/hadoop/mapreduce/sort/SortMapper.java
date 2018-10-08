package com.tf.hadoop.mapreduce.sort;
/**
 * 文本排序-map分组，在Hadoop默认的排序算法中，只会针对key值进行排序
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();//获取文本内容
        context.write(new Text(line),NullWritable.get());
    }
}
