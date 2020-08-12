package com.atguigu.job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-08
 */
public class OneIndexReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //累加求和
        for(IntWritable value:values){
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
