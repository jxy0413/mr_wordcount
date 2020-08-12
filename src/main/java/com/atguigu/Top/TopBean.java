package com.atguigu.Top;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-08
 */
public class TopBean implements WritableComparable<TopBean> {
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    int result;
    @Override
    public int compareTo(TopBean bean) {
        if(this.sumFlow>bean.sumFlow){
            return 1;
        }else if(this.sumFlow==bean.sumFlow){
            return 0;
        }else{
            return -1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
           out.writeLong(upFlow);
           out.writeLong(downFlow);
           out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
          this.upFlow = in.readLong();
          this.downFlow = in.readLong();
          this.sumFlow = in.readLong();
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public TopBean(){

    }

    public TopBean(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow=upFlow+downFlow;
    }

    @Override
    public String toString() {
        return "upFlow" + "\t"+upFlow +
                ", downFlow" +"\t"+ downFlow +
                ", sumFlow" +"\t"+ sumFlow;
    }
}
