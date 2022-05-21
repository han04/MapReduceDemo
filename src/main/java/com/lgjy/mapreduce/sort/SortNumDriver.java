package com.lgjy.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class SortNumDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //配置job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(SortNumDriver.class);
        //关联mapper和reducer
        job.setMapperClass(SortNumMapper.class);
        job.setReducerClass(SortNumReducer.class);
        //设置map输出的key value的类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输出的key value的类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //配置输入、输出路径
        FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop102:/input/sortnum"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop102/output/sortnum"));
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1 );
    }
}
