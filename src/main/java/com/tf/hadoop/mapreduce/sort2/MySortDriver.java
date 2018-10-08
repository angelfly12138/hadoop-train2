package com.tf.hadoop.mapreduce.sort2;
/**
 * 自定义排序驱动程序
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MySortDriver extends Configured implements Tool {
    public int run(String[] arg0) throws Exception {
        if (arg0.length != 2){
            System.err.printf("Usage:%s[generic options]<input> <output>\n",getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
//      Configuration conf = new Configuration();
//      Job job = new Job(getConf(),"Max Temperture");
        Job job = Job.getInstance(getConf(), "MySort");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

        job.setMapperClass(MySortMapper.class);
        job.setReducerClass(MySortReducer.class);

        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception {
        // 创建Configuration
        Configuration configuration = new Configuration();
        // 准备清理已存在的输出目录
        Path outputpath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputpath)){
            fileSystem.delete(outputpath,true);
            System.out.println("Output file exists,but is has deleted!");
        }
        int exitCode = ToolRunner.run(new MySortDriver(), args);
        System.exit(exitCode);
    }
}
