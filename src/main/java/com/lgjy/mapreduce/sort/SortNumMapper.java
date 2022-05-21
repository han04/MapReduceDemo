package com.lgjy.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortNumMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private IntWritable outK = new IntWritable();
    private IntWritable outV = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        outK.set(Integer.parseInt(line));
        context.write(outK, outV);
    }
}

