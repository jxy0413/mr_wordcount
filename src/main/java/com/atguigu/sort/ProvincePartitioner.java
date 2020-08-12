package com.atguigu.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Auther jxy
 * @Date 2020-08-05
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text value, int numPartitions) {
        int partion = 2;
        String preNum = value.toString();
        if("136".equals(preNum)){
            partion = 1;
        }else if("137".equals(preNum)){
            partion = 0;
        }
        return partion;
    }
}
