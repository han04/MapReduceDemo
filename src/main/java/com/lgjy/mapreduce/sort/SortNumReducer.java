package com.lgjy.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortNumReducer extends Reducer<IntWritable, IntWritable, IntWritable,IntWritable> {
    private  IntWritable num = new IntWritable(1);//排名
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            context.write(num,key);//写入排名和数字 将输入的key作为输出的key
            System.out.println(num+"\t"+key);// 拼接数字与排名
            num = new IntWritable(num.get()+1);//排名+1
        }
    }
}
