package com.tf.hadoop.mapreduce.top;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 接收到的每一行数据
        String line = value.toString();

        //按照指定分隔符进行拆分
        String[] words = line.split(",");

        context.write(new Text(words[0]), new LongWritable(Long.parseLong(words[1])));
    }
}
