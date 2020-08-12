package com.atguigu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


/**
 * @Auther jxy
 * @Date 2020-07-30
 */
public class WordCountDriver  {
    public static void main(String[] args) throws IOException {
        args = new String[]{"/Users/jiaxiangyu/Desktop/1.txt","/Users/jiaxiangyu/Desktop/output95"};
        //1。获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2。设置jar存储位置
        job.setJarByClass(WordCountDriver.class);
        //3。关联Map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4。设置Mapper阶段输出的k、v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5。设置最终数据输出的k，v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(WordCountCombiner.class);
        //6。设置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7提交
        try {
           job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
