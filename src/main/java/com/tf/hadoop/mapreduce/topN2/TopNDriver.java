package com.tf.hadoop.mapreduce.topN2;


import com.tf.hadoop.mapreduce.topN2.utils.MapReduceJobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopNDriver {
    public static void main(String[] args) throws Exception {
        // 确保有3个确切的参数
        if (args == null || args.length < 3) {
            System.err.println("Parameter Errors! Usages:<topN> <inputpath> <outputpath>");
            System.exit(-1);
        }

        Configuration configuration = new Configuration();
        configuration.set("topN", args[0]);

        Job job = MapReduceJobUtil.buildJob(configuration,
                TopNDriver.class,
                args[1],
                TextInputFormat.class,
                TopNMapper.class,
                IntWritable.class,
                NullWritable.class,
                new Path(args[2]),
                TextOutputFormat.class,
                TopNReducer.class,
                IntWritable.class,
                IntWritable.class
        );
        //将运行进度等信息及时输出给用户
        boolean result = job.waitForCompletion(true);





    }

}
