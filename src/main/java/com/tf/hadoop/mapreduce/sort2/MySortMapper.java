package com.tf.hadoop.mapreduce.sort2;
/**
 * 自定义排序map端
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class MySortMapper extends Mapper<LongWritable, Text,SortBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value,Mapper<LongWritable, Text,SortBean, NullWritable>.Context context)throws IOException, InterruptedException{
        String line = value.toString();
        String []num = line.split("\t");
        long firstNum = Long.parseLong(num[0]);
        long secondNum = Long.parseLong(num[1]);
        SortBean bean = new SortBean(firstNum,secondNum);
        context.write(bean,NullWritable.get());
    }
}
