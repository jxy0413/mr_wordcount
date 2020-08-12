package com.atguigu.sort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther jxy
 * @Date 2020-08-05
 */
public class FlowCountDriver {
    public static void main(String[] args)throws Exception{
        args = new String[]{"/Users/jiaxiangyu/Desktop/input/sort/","/Users/jiaxiangyu/Desktop/output53"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCountDriver.class);

        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        //指定map kv
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(3);

        //指定输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }
}
