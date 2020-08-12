package com.atguigu.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther jxy
 * @Date 2020-08-02
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFilw = 0;
        //累计求和
       for(FlowBean flowBean:values){
           sum_downFilw += flowBean.getDownFlow();
           sum_upFlow += flowBean.getUpFlow();
       }
       v.set(sum_upFlow,sum_downFilw);

       //写出
       context.write(key,v);
    }
}
