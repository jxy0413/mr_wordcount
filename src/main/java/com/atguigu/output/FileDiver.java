package com.atguigu.output;

import com.atguigu.NLine.NLineDriver;
import com.atguigu.NLine.NLineMapper;
import com.atguigu.NLine.NLineReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FilterOutputFormat;

/**
 * @Auther jxy
 * @Date 2020-08-06
 */
public class FileDiver {
    public static void main(String[] args)throws Exception {
        args = new String[] {"/Users/jiaxiangyu/Desktop/input/line/","/Users/jiaxiangyu/Desktop/output21"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //使用
        job.setJarByClass(FileDiver.class);

        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //自定义要输出的地方
        job.setOutputFormatClass(FilterOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
