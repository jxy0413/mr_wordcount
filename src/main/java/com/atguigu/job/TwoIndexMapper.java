package com.atguigu.job;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-08
 */

public class TwoIndexMapper extends Mapper<LongWritable, Text,Text,Text> {
    //atguigu--1.txt	3
    //atguigu--2.txt	2
    Text k = new Text();
    Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] filed = line.split("--");
        k.set(filed[0]);
        v.set(filed[1]);
        context.write(k,v);
    }
}
