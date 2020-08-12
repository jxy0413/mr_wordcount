package com.atguigu.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-04
 */
public class WholeRecordReader extends RecordReader<Text, ByteWritable>{

    FileSplit split;

    Configuration configuration;

    Text k = new Text();

    ByteWritable v = new ByteWritable();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //初始化
        this.split = (FileSplit) split;

        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //核心的业务逻辑
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public ByteWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        //1、获取路径
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(configuration);

        //2、获取输入流

        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
