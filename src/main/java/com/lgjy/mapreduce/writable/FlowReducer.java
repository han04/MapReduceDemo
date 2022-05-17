package com.lgjy.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer <Text,FlowBean, Text,FlowBean>{
    private FlowBean outV = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        long totalUp = 0;
        long totalDown = 0;
        //遍历集合 累加
        for(FlowBean value :values){
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();

        }

        //封装outK outV
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        //写出
        context.write(key,outV);
    }
}
