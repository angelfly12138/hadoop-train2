package com.tf.hadoop.mapreduce.topN;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private int N = 10;// 默认取top 10
    private SortedMap<Integer, String> top = new TreeMap<Integer, String>();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String[] splits = StringUtils.split(row,",");//分割
        int frequency = Integer.valueOf(splits[0]);// 频率
        top.put(frequency,row);

        // 只保留top N
        if (top.size() > N) {
            // 删除频度最小的元素
            top.remove(top.firstKey());
            // 删除频度最大的元素
//            top.remove(top.lastKey());
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 默认取top 10
        this.N = context.getConfiguration().getInt("N", 10);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String str : top.values()) {
            context.write(NullWritable.get(), new Text(str));
        }
    }
}
