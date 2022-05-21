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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SortNumDriver.class);

        job.setMapperClass(SortNumMapper.class);
        job.setReducerClass(SortNumReducer.class);

//      job.setMapOutputKeyClass(IntWritable.class);
//      job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop102:/input/sortnum"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop102/output/sortnum"));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1 );
    }
}
