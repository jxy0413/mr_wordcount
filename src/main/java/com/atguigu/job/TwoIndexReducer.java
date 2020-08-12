package com.atguigu.job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-08
 */
public class TwoIndexReducer extends Reducer<Text,Text,Text,Text> {
    //atguigu    --1.txt	3
    //           --2.txt	2
    Text v = new Text();
    //最终的  atguigu 1.txt-->2 b.txt-->2
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for(Text value:values){
            sb.append(value.toString().replace("\t","-->")+"\t");
        }
        //写出
        v.set(sb.toString());
        context.write(key,v);
    }
}
