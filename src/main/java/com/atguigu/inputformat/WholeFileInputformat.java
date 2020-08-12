package com.atguigu.inputformat;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-04
 */
public class WholeFileInputformat extends FileInputFormat<Text, ByteWritable>{
    @Override
    public RecordReader<Text, ByteWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();

        return null;
    }
}
