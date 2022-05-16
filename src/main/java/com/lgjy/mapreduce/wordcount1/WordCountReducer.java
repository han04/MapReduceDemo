package com.lgjy.mapreduce.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 *KEYIN  reduce 阶段输入的Key的类型：LongWritable
 *VALUEIN reduce 阶段输入Value类型： Text
 *KEYOUT  reduce 阶段输出的Key类型： Text
 *VALUEOUT reduce 阶段输出的Value类型： IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    IntWritable outV = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();

        }

        outV.set(sum);
        context.write(key,outV);
    }
}
