package com.tf.hadoop.mapreduce.topN2.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceJobUtil {
    public static Job buildJob(Configuration conf,
                               Class<?> jobClass,
                               String inputpath,
                               Class<? extends InputFormat> inputFormat,
                               Class<? extends Mapper> mapperClass,
                               Class<?> mapKeyClass,
                               Class<?> mapValueClass,
                               Path outputpath,
                               Class<? extends OutputFormat> outputFormat,
                               Class<? extends Reducer> reducerClass,
                               Class<?> outkeyClass,
                               Class<?> outvalueClass) throws IOException {

        String jobName = jobClass.getSimpleName();
        Job job = Job.getInstance(conf, jobName);
        //设置job运行的jar
        job.setJarByClass(jobClass);
        //设置整个程序的输入
        FileInputFormat.setInputPaths(job, inputpath);
        job.setInputFormatClass(inputFormat);//设置如何将输入文件解析成一行一行内容的解析类
        //设置mapper
        job.setMapperClass(mapperClass);
        job.setMapOutputKeyClass(mapKeyClass);
        job.setMapOutputValueClass(mapValueClass);
        //设置整个程序的输出
        outputpath.getFileSystem(conf).delete(outputpath, true);//如果当前输出目录存在，删除之，以避免.FileAlreadyExistsException
        FileOutputFormat.setOutputPath(job, outputpath);
        job.setOutputFormatClass(outputFormat);
        //设置reducer，如果有才设置，没有的话就不用设置
        if (null != reducerClass) {
            job.setReducerClass(reducerClass);
            job.setOutputKeyClass(outkeyClass);
            job.setOutputValueClass(outvalueClass);
        }
        return job;
    }
}
