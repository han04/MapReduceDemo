package com.lgjy.mapreduce.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortNumMapper extends Mapper<LongWritable, Text,SortBean, NullWritable> {
    private SortBean outK = new SortBean();


    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, SortBean, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\n");

        for(String num : split){

            outK.setNum(Integer.parseInt(num));
            context.write(outK,NullWritable.get());
        }


    }
}

