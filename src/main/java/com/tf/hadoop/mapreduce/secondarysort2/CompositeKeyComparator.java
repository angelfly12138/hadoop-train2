package com.tf.hadoop.mapreduce.secondarysort2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 实现组合键排序
 */
public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator(){
        super(CompositeKey.class,true);
    }

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {
        CompositeKey ck1 = (CompositeKey)wc1;
        CompositeKey ck2= (CompositeKey)wc2;

        /**
         * 比较ck1和ck2，并返回
         * 0，如果ck1和ck2相等
         * 1，如果ck1>ck2
         * -1,如果ck1<ck2
         */
        int comparison = ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
        if(comparison == 0){
            if(ck1.getTimestamp() == ck2.getTimestamp()){
                return 0;
            }
            else if(ck1.getTimestamp() < ck2.getTimestamp()){
                return -1;
            }
            else{
                return 1;
            }
        }
        else {
            return comparison;
        }
    }
}
