package com.tf.hadoop.mapreduce.secondarysort2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义一个自然值
 */
public class NaturalValue implements Writable, Comparable<NaturalValue> {

    private long timestamp;
    private double price;

    public NaturalValue() {
    }

    public NaturalValue(long timestamp, double price) {
        set(timestamp, price);
    }

    public void set(long timestamp, double price) {
        this.timestamp = timestamp;
        this.price = price;
    }

    public static NaturalValue copy(NaturalValue value) {
        return new NaturalValue(value.timestamp, value.price);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * 将二进制数据转换成NaturalValue
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static NaturalValue read(DataInput in) throws IOException {
        NaturalValue value = new NaturalValue();
        value.readFields(in);
        return value;
    }

    public String getDate() {
        return DateUtil.getDateAsString(this.timestamp);
    }

    /**
     * 创建此对象的clone
     *
     * @param
     * @return
     */
    public NaturalValue clone() {
        return new NaturalValue(timestamp, price);
    }

    @Override
    public int compareTo(NaturalValue data) {
        if (this.timestamp < data.timestamp) {
            return -1;
        } else if (this.timestamp > data.timestamp) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.timestamp);
        out.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.timestamp = in.readLong();
        this.price = in.readDouble();
    }
}
