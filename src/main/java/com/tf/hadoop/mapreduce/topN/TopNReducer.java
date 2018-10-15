package com.tf.hadoop.mapreduce.topN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text> {
    private int N = 10;// 默认取top 10
    private SortedMap<Integer, String> top = new TreeMap<Integer, String>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String valueAsString = value.toString().trim();
            String[] tokens = valueAsString.split(",");
            int frequency = Integer.parseInt(tokens[0]);
            top.put(frequency, valueAsString);
            // 只保留top N
            if (top.size() > N) {
                // 删除频度最小的元素
                top.remove(top.firstKey());
                // 删除频度最大的元素
//            top.remove(top.lastKey());
            }
        }
        // 发出最终的top 10 列表
        List<Integer> keys = new ArrayList<Integer>(top.keySet());
        for (int i = keys.size() - 1; i >= 0; i--) {
            context.write(new IntWritable(keys.get(i)),new Text(top.get(keys.get(i))));
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N",10);// 默认取top 10
    }
}
