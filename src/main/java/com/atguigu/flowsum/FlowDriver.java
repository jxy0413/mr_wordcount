package com.atguigu.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @Auther jxy
 * @Date 2020-08-02
 */
public class FlowDriver  {
    public static void main(String[] args) throws Exception{
        args = new String[]{"/Users/jiaxiangyu/Desktop/1.txt","/Users/jiaxiangyu/Desktop/output9"};
        //获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar路径
        job.setJarByClass(FlowDriver.class);
        //关联mapper和reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowReducer.class);
        //设置mapper输出的key和value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //最终输出
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}
