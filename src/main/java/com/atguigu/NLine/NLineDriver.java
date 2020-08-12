package com.atguigu.NLine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther jxy
 * @Date 2020-08-04
 */
public class NLineDriver {
    public static void main(String[] args) throws Exception{

        args = new String[] {"/Users/jiaxiangyu/Desktop/input/line/","/Users/jiaxiangyu/Desktop/output21"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //设置每个记录 一个切片
        NLineInputFormat.setNumLinesPerSplit(job,3);

        //使用
        job.setInputFormatClass(NLineInputFormat.class);

        job.setJarByClass(NLineDriver.class);

        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
