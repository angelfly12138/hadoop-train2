package com.tf.hadoop.mapreduce.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopNBoot {
	
	/**
	 * @param args
	 *        args[0] inputfile path
	 *        args[1] output path
     *        args[2] output2 path
	 *        args[3] key index
	 *        args[4] value index
	 *        args[5] topN value, default is 5
	 *        args[6] delimiter, default is ','
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		if(args.length < 7) {
			throw new Exception("the total of args invaild");
		}
		
		Configuration conf = new Configuration();
		
		if("".equals(args[0])) {
			throw new Exception("inputfile path not allow to Empty");
		}
		Path inputPath = new Path(args[0]);
		
		if("".equals(args[1])) {
			throw new Exception("output path not allow to Empty");
		}
        if("".equals(args[2])) {
            throw new Exception("output2 path not allow to Empty");
        }
		Path outputPath = new Path(args[1]);
		Path outputPath2 = new Path(args[2]);
		
		conf.setInt("key_index", Integer.valueOf(args[3]));
		conf.setInt("value_index", Integer.valueOf(args[4]));
		if(!"".equals(args[5])) {
			conf.setInt("topN", Integer.valueOf(args[5]));
		}
		if(!"".equals(args[6])) {
			conf.set("delimiter", args[6]);
		}


        //创建Job
        Job job = Job.getInstance(conf, "wordcount");


        //设置job的处理类
        job.setJarByClass(TopNBoot.class);

        //设置作业处理的输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //设置map相关参数
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置reduce相关参数
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置job的partition
        job.setPartitionerClass(WordCountPartition.class);
        //设置4个reducer，每个分区一个
        job.setNumReduceTasks(4);

        //设置作业处理的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //如果输出目录已存在则删除
        outputPath.getFileSystem(conf).delete(outputPath, true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        boolean result = job.waitForCompletion(true);
        if (!result) {
            System.out.println("WordCount Error!");
        }
        else {
//            Job job1 = new Job(conf);
            Job job1 = Job.getInstance(conf,"topN");
            job1.setJarByClass(TopNBoot.class);

            job1.setMapperClass(TopNMapper.class);
            job1.setReducerClass(TopNReducer.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(IntWritable.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);

            job1.setInputFormatClass(TextInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job1, outputPath);
            FileOutputFormat.setOutputPath(job1, outputPath2);

            //如果输出目录已存在则删除
            outputPath2.getFileSystem(conf).delete(outputPath2, true);

            System.exit(job1.waitForCompletion(true) ? 1 : 0);

        }

	}

}
