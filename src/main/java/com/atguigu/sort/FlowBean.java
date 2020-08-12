package com.atguigu.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-05
 */
public class FlowBean implements WritableComparable<FlowBean>{

    private long upFlow; //上行流量
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow+downFlow;
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

    @Override
    public int compareTo(FlowBean o) {
        int result;
        //核心比较条件
        if(sumFlow>o.getSumFlow()){
            result = -1;
        }else if(sumFlow<o.getSumFlow()){
            result = 1;
        }else {
            result = 0;
        }

        return 0;
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
         upFlow=in.readLong();
         downFlow=in.readLong();
         sumFlow=in.readLong();
    }

    @Override
    public String toString() {
        return
                 upFlow + "\t"+
                 downFlow + "\t" + sumFlow ;
    }
}
