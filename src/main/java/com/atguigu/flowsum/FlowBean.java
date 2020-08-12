package com.atguigu.flowsum;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-01
 */

public class FlowBean implements Writable{
    private long upFlow; //上行流量
    private long downFlow; //下行流量
    private long sumFlow; //总流量

    public FlowBean(){
        super();
    }

    public FlowBean(long upFlow,long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        sumFlow = upFlow+downFlow;
    }

    @Override
    public String toString() {
        return
                upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
         out.writeLong(upFlow);
         out.writeLong(downFlow);
         out.writeLong(sumFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        //顺序必须一致
         upFlow = in.readLong();
         downFlow = in.readLong();
         sumFlow = in.readLong();
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
    }
}