package com.lgjy.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SortBean, NullWritable, IntWritable,NullWritable> {
    private IntWritable outK =new IntWritable();
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Reducer<SortBean, NullWritable, IntWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(key.getNum()),NullWritable.get());
    }
}
