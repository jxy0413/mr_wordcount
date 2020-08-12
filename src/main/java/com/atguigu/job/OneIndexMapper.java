package com.atguigu.job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-08
 */
public class OneIndexMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    String name;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名称
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    Text k = new Text();
    IntWritable v = new IntWritable(1);
    //atguigu ss
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        //切割
        String[] fileds = line.split(" ");

        //写出
        for(String word:fileds){
            k.set(word+"--"+name);
            context.write(k,v);
        }

    }
}
