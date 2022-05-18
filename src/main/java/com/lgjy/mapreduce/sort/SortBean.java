package com.lgjy.mapreduce.sort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {
    private int num ;
    public SortBean(){

    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.num = dataInput.readInt();

    }


    @Override
    public int compareTo(SortBean sortBean) {

        int result = this.num - sortBean.num;
        return result;
    }
}
