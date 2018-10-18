package com.tf.hadoop.mapreduce.topN2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class TopNMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    //private Map<String, Integer> topMap = new TreeMap<String, Integer>();
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
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 解析每一行字符串
        String row = value.toString();
        String[] splits = StringUtils.split(row, ",");// 注意StringUtils导包正确
        // 剔除异常的数据
        if (splits == null || splits.length < 5) {
            return;
        }
        // 转换所要那一列为int
        int frequency = Integer.valueOf(splits[3]);
        // 将数字写入到TreeSet当中
        cachedTopN.add(frequency);
        // 判断cachedTopN中的元素个数是否已经达到N个，如果已经达到N个，则去掉最后一个
        if (cachedTopN.size() > N) {
            cachedTopN.pollLast();
        }
    }
    /**
     * 将map函数筛选出来的前N个数写入到context中作为输出
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer num : cachedTopN){
            context.write(new IntWritable(num), NullWritable.get());
        }
    }
}
