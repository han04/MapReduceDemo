package com.lgjy.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortNumReducer extends Reducer<IntWritable, IntWritable, IntWritable,IntWritable> {
    private  IntWritable num = new IntWritable(1);
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            context.write(num,key);
            System.out.println(num+"\t"+key);
            num = new IntWritable(num.get()+1);
        }
    }
}
