package com.tf.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 *  驱动器，定义输入、输出，并注册插件类
 */
public class SecondarySortDriver extends Configured implements Tool {
    private static Logger theLogger = Logger.getLogger(SecondarySortDriver.class);
    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        Job job = new Job(configuration);
        job.setJarByClass(SecondarySortDriver.class);
        job.setJobName("SencondarySortDriver");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        theLogger.info("run(): status="+status);
        return status ? 0 : 1;
    }
    /**
     * 二次排序MapReduce程序的主驱动器
     * 调用这个方法提交MapReduce作业
     * @throws Exception 如果与作业跟踪器存在通信问题，会抛出异常
     */
    public static void main(String[] args) throws Exception{
        // 确保有2个参数
        if(args.length != 2){
            theLogger.warn("SecondarySortDriver <input-dir> <output-dir>");
            throw new IllegalArgumentException("SecondarySortDriver <input-dir> <output-dir>");
        }
        //String inputDir = args[0];
        //String outputDir = args[1];
        int returnStatus = submitJob(args);
        theLogger.info("returnStatus="+returnStatus);

        System.exit(returnStatus);
    }

    private static int submitJob(String[] args) throws Exception{
        //String[] args = new String[2];
        //args[0] = inputDir;
        //args[1] = outputDir;
        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
        return returnStatus;
    }
}
