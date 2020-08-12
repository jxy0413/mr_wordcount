package com.atguigu.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-06
 */
public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    String name;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       //获取文件的名称
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    TableBean tableBean = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // id   pid   amount
        // 1001  01    1

        //pid name
        // 01 小米
        if(name.startsWith("order")){ //订单表
            String[] fields = line.split(" ");
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setPname("");
            tableBean.setFlag("order");

            k.set(fields[1]);
        }else{
            String[] fields = line.split(" ");
            tableBean.setPid(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setId("");
            tableBean.setAmount(0);
            tableBean.setFlag("pd");
            k.set(fields[0]);
        }
        context.write(k,tableBean);
    }
}
