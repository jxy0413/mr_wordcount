package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-07-30
 * map阶段
 * atguigu,1 atguigu,1
 */
public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        //获取一行 偏移量
        String line = value.toString();
        //切割单词
        String[] words = line.split(" ");
        System.out.println(key.toString());
        System.out.println(value.toString());
        //循环写出
        for(String word:words){
            k.set(word);
            context.write(k,v);
        }
    }
}
