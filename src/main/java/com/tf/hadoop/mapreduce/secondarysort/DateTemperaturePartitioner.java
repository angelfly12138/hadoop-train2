package com.tf.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {

    @Override
    public int getPartition(DateTemperaturePair pair, Text text, int numberOfPartitions) {
        // 确保partition是非负
        return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
    }
}
