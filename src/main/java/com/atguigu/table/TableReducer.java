package com.atguigu.table;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @Auther jxy
 * @Date 2020-08-06
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> tableBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for(TableBean tableBean:values){
            if("order".equals(tableBean.getFlag())){
                TableBean temBean = new TableBean();
                try {
                    BeanUtils.copyProperties(temBean,tableBean);
                    tableBeans.add(temBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else{
                try {
                    BeanUtils.copyProperties(pdBean,tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for(TableBean t:tableBeans){
            t.setPname(pdBean.getPname());
            context.write(t,NullWritable.get());
        }

    }
}
