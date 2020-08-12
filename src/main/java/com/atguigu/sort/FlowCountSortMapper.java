package com.atguigu.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-05
 */
public class FlowCountSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    FlowBean k = new FlowBean();
    Text v =  new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 获取一行
        String line = value.toString();
        //2。切割
        String[] fields = line.split(" ");
        //3、封装对象
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);

        System.out.println(k);

        v.set(phoneNum);

        context.write(k,v);
    }
}
