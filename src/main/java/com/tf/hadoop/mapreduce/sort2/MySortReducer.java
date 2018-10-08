package com.tf.hadoop.mapreduce.sort2;
/**
 *  自定义排序reduce端
 */

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MySortReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values,
                          Reducer<SortBean, NullWritable, SortBean, NullWritable>.Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}