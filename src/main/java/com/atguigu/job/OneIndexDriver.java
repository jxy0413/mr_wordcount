package com.atguigu.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther jxy
 * @Date 2020-08-08
 */
public class OneIndexDriver {
    public static void main(String[] args)throws Exception {
        args = new String[]{"/Users/jiaxiangyu/Desktop/input/job/","/Users/jiaxiangyu/Desktop/input/jobout3/"};
        //1。获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2。设置jar存储位置
        job.setJarByClass(OneIndexDriver.class);
        //3。关联Map和reduce类
        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OneIndexReducer.class);
        //4。设置Mapper阶段输出的k、v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5。设置最终数据输出的k，v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
