package com.tf.hadoop.mapreduce.sort2;
/**
 * 实现WritableComparable接口，重写compareTo进行排序
 */

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class SortBean implements WritableComparable<SortBean> {
    private long firstNum;
    private long secondNum;

    public SortBean() {
    }

    public SortBean(long firstNum, long secondNum) {
        this.firstNum = firstNum;
        this.secondNum = secondNum;
    }

    @Override
    public String toString() {
        return "SortBean{" +
                "firstNum=" + firstNum +
                ", secondNum=" + secondNum +
                '}';
    }

    public long getFirstNum() {
        return firstNum;
    }

    public void setFirstNum(long firstNum) {
        this.firstNum = firstNum;
    }

    public long getSecondNum() {
        return secondNum;
    }

    public void setSecondNum(long secondNum) {
        this.secondNum = secondNum;
    }

    public int compareTo(SortBean o) {
        // 返回1 则交换，返回-1 则不交换
        if (this.firstNum == o.getFirstNum()) {
            return this.secondNum > o.getSecondNum() ? 1 : -1;
        } else {
            return this.firstNum > o.getFirstNum() ? 1 : -1;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(firstNum);
        dataOutput.writeLong(secondNum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.firstNum = dataInput.readLong();
        this.secondNum = dataInput.readLong();
    }
}
