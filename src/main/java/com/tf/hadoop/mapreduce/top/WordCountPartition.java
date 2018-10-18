package com.tf.hadoop.mapreduce.top;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartition extends Partitioner<Text, LongWritable> {


    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) {

        return text.hashCode() % numPartitions;
    }
}
