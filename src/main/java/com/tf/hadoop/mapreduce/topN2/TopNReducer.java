package com.tf.hadoop.mapreduce.topN2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

public class TopNReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
    TreeSet<Integer> cachedTopN = null;
    Integer N = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // TreeSet定义的排序规则为倒序，后面做数据的处理时只需要pollLast最后一个即可将TreeSet中较小的数去掉
        cachedTopN = new TreeSet<Integer>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                int ret = 0;
                if (o1 > o2) {
                    ret = -1;
                } else if (o1 < o2) {
                    ret = 1;
                }
                return ret;
            }
        });
        // 拿到传入参数时的topN中的N值
        N = Integer.valueOf(context.getConfiguration().get("topN"));

    }

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 将map传过来的数据写入到TreeSet中
        cachedTopN.add(Integer.valueOf(key.toString()));
        // 判断cachedTopN中的元素个数是否已经达到N个，如果已经达到N个，则去掉最后一个
        if (cachedTopN.size() > N) {
            cachedTopN.pollLast();
        }

    }
    /**
     * 将reduce函数筛选出来的前N个数写入到context中作为输出
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int index = 1;
        for(Integer num : cachedTopN) {
            context.write(new IntWritable(index), new IntWritable(num));
            index++;
        }

    }
}
