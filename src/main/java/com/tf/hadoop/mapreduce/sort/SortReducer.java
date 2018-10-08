package com.tf.hadoop.mapreduce.sort;
/**
 *   文本排序-reduce端排序，在Hadoop默认的排序算法中，只会针对key值进行排序
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable,Text,NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
