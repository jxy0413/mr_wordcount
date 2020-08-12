package com.atguigu.compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.*;

/**
 * @Auther jxy
 * @Date 2020-08-07
 */
public class TestCompress {
    public static void main(String[] args) throws Exception{
        //压缩
        compressBzip("","org.apache.hadoop.io.compress.BZip2Codec");
        decomptess("");
    }

    private static void decomptess(String fileName) {
        //合法性
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));
        if(codec==null){
            return;
        }

        try {
            FileInputStream fis = new FileInputStream(new File(fileName));
            CompressionInputStream cis = null;
            try {
                cis = codec.createInputStream(fis);
                FileOutputStream fos = new FileOutputStream(new File(fileName + ".decode"));

                IOUtils.copyBytes(cis,fos,1024*1024,false);

                IOUtils.closeStream(fos);
                IOUtils.closeStream(cis);
                IOUtils.closeStream(fis);
            } catch (IOException e) {
                e.printStackTrace();
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


    }

    private static void compressBzip(String fileName, String method) throws Exception{
        FileInputStream fs = new FileInputStream(new File(fileName));

        Class<?> codecClass = Class.forName(method);

        CompressionCodec codec=(CompressionCodec)ReflectionUtils.newInstance(codecClass,new Configuration());

        //获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName+codec.getDefaultExtension()));

        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fs,cos,1024*1024,false);

        //关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fs);
    }
}
