package com.tf.hadoop.mapreduce.topN;

import com.tf.hadoop.mapreduce.secondarysort2.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class TopNDriver extends Configured implements Tool {
    private static Logger THE_LOGGER = Logger.getLogger(TopNDriver.class);

    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("","");
        Job job = new Job(getConf());
        HadoopUtil.addJarsToDistributedCache(job, "/lib/");
        int N = Integer.parseInt(args[0]); // top N
        job.getConfiguration().setInt("N", N);
        job.setJobName("TopNDriver");
        job.setJarByClass(TopNDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);

        // map()'s output (K,V)
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // reduce()'s output (K,V)
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // args[1] = input directory
        // args[2] = output directory
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean status = job.waitForCompletion(true);
        THE_LOGGER.info("run(): status="+status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        // 确保这里有3个确切的参数
        if (args.length != 3){
            THE_LOGGER.warn("usage TopNDriver <N> <input> <output>");
            System.exit(1);
        }
        THE_LOGGER.info("N="+args[0]);
        THE_LOGGER.info("inputDir="+args[1]);
        THE_LOGGER.info("outputDir="+args[2]);
        int returnStatus = ToolRunner.run(new TopNDriver(), args);
        System.exit(returnStatus);
    }
}
