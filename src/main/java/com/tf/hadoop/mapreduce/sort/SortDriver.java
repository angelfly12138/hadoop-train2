package com.tf.hadoop.mapreduce.sort;
/**
 *  排序驱动程序
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortDriver extends Configured implements Tool {
    public int run(String[] arg0) throws Exception {
        if (arg0.length != 2) {
            System.out.printf("Usage:%s[generic options]<input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
//      Configuration conf = new Configuration();
//      Job job = new Job(getConf(),"Max Temperture");
        Job job = Job.getInstance(getConf(), "Sort");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new SortDriver(),args);
        System.exit(exitCode);
    }
}
