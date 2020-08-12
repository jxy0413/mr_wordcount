package com.atguigu.kvwordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Auther jxy
 * @Date 2020-08-03
 */
public class KVTestDriver {
    public static void main(String[] args) throws Exception{
        args = new String[]{"/Users/jiaxiangyu/Desktop/input/kv/","/Users/jiaxiangyu/Desktop/output11"};

        //1.获取job对象
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(conf);
        //设置jar路径
        job.setJarByClass(KVTestDriver.class);
        //关联mapper reducer
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTestReducer.class);
        //设置mapper key value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //最终输出 key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //输出 输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
        job.waitForCompletion(true);
    }
}
