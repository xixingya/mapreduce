package site.xixing.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/19 10:23
 */
public class TestCompress {
    public static void main(String[] args) throws Exception {
        //testCompress("F:/《数学建模算法与应用》（第2版）.pdf","org.apache.hadoop.io.compress.BZip2Codec");
        testDeCompress("F:/《数学建模算法与应用》（第2版）.pdf.bz2");
    }

    private static void testDeCompress(String name) throws Exception {
        //1.获取输入流
        FileInputStream fis=new FileInputStream(name);
        //2.获取编码器
        Configuration conf = new Configuration();

        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = compressionCodecFactory.getCodec(new Path(name));
        if(codec==null){
            System.out.println("can not find codec for this file");
            return;
        }
        //3.获取压缩流
        CompressionInputStream cis = codec.createInputStream(fis);
        //4.获取输出流
        FileOutputStream fos=new FileOutputStream(name+".decode");
        //5.流的拷贝
        IOUtils.copyBytes(cis,fos,conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);


    }

    /**
     *
     * @param name 文件名
     * @param method  方式
     */

    private static void testCompress(String name, String method) throws Exception {

        //获取输入流
        FileInputStream fis=new FileInputStream(name);
        //2.获取编解码器
        Class<?> clazz = Class.forName(method);
        Configuration configuration = new Configuration();
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clazz, configuration);
        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(name + codec.getDefaultExtension());
        //4.获取压缩流
        CompressionOutputStream cos = codec.createOutputStream(fos);
        //5.流的拷贝
        IOUtils.copyBytes(fis,fos,configuration);
        //流的关闭
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

    }
}
