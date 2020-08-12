package com.atguigu.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-06
 */
public class FRecordWriter extends RecordWriter<Text, NullWritable>{
    FSDataOutputStream fosAtguigu;
    FSDataOutputStream fosOther;
    public FRecordWriter(TaskAttemptContext job) {
        //1 获取文件系统
        try {
            FileSystem fs= FileSystem.get(job.getConfiguration());
            fosAtguigu = fs.create(new Path("/"));
            fosOther = fs.create(new Path(""));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //2 创建输出到atguigu.log的输出流
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断key是否有
        if(key.toString().contains("atguigu")){
             fosAtguigu.write(key.toString().getBytes());
        }else{
             fosOther.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fosAtguigu);
        IOUtils.closeStream(fosOther);
    }
}
