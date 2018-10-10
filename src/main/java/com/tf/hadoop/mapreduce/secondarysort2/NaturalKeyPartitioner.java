package com.tf.hadoop.mapreduce.secondarysort2;

import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 实现自然键分区
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NaturalValue> {

    @Override
    public int getPartition(CompositeKey key, NaturalValue value, int numberOfPartitions) {
        return Math.abs((int)(hash(key.getStockSymbol()) % numberOfPartitions));
    }
    /**
     *  改写 String.hashCode()
     */
    static long hash(String str) {
        long h = 1125899906842597L;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31*h + str.charAt(i);
        }
        return h;
    }
}
