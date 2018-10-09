package com.tf.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  将日期和温度对 定义为java对象
 */
public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {
    private final Text yearMonth = new Text();
    private final Text day = new Text();
    private final IntWritable temperature = new IntWritable();


    public DateTemperaturePair() {
    }

    public DateTemperaturePair(String yearMonth, String day, int temperature) {
        this.yearMonth.set(yearMonth);
        this.day.set(day);
        this.temperature.set(temperature);
    }

    public static DateTemperaturePair read(DataInput in) throws IOException {
        DateTemperaturePair pair = new DateTemperaturePair();
        pair.readFields(in);
        return pair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        yearMonth.write(out);
        day.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        day.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public int compareTo(DateTemperaturePair pair) {
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(pair.getTemperature());
        }
        //return compareValue; 		// 升序排序
        return -1*compareValue;     // 降序排序
    }

    public Text getYearMonthDay() {
        return new Text(yearMonth.toString()+day.toString());
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public Text getDay() {
        return day;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public void setYearMonth(String yearMonthAsString) {
        yearMonth.set(yearMonthAsString);
    }

    public void setDay(String dayAsString) {
        day.set(dayAsString);
    }

    public void setTemperature(int temp) {
        temperature.set(temp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DateTemperaturePair that = (DateTemperaturePair) o;
        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
            return false;
        }
        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DateTemperaturePair{yearMonth=");
        builder.append(yearMonth);
        builder.append(", day=");
        builder.append(day);
        builder.append(", temperature=");
        builder.append(temperature);
        builder.append("}");
        return builder.toString();
    }
}
