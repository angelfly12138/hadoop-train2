package com.tf.hadoop.mapreduce.secondarysort2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义一个组合键
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    private String stockSymbol;
    private long timestamp;

    public CompositeKey() {
    }

    public void set(String stockSymbol, long timestamp) {
        this.stockSymbol = stockSymbol;
        this.timestamp = timestamp;
    }
    public CompositeKey(String stockSymbol, long timestamp) {
        set(stockSymbol,timestamp);
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public void setStockSymbol(String stockSymbol) {
        this.stockSymbol = stockSymbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(CompositeKey other) {
        if (this.stockSymbol.compareTo(other.stockSymbol) != 0) {
            return this.stockSymbol.compareTo(other.stockSymbol);
        }
        else if (this.timestamp != other.timestamp) {
            return timestamp < other.timestamp ? -1 : 1;
        }
        else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.stockSymbol);
        out.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.stockSymbol = in.readUTF();
        this.timestamp = in.readLong();
    }

    public static class CompositeKeyComparator extends WritableComparator{
        public CompositeKeyComparator(){
            super(CompositeKey.class);
        }
        public int compare(byte[] b1,int s1,int l1,byte[] b2, int s2,int l2){
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }
    static {
        WritableComparator.define(CompositeKey.class,new com.tf.hadoop.mapreduce.secondarysort2.CompositeKeyComparator());
    }
}