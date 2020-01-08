package site.xixing.douban;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import site.xixing.flow.FlowBean;
import site.xixing.flow.FlowDriver;
import site.xixing.flow.FlowMapper;
import site.xixing.flow.FlowReducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2020/1/5 10:45
 */
public class DoubanDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包
        job.setJarByClass(DoubanDriver.class);
        //3.设置关联的包
        job.setMapperClass(DoubanMapper.class);
        job.setReducerClass(DoubanReducer.class);
        //设置mappper输出数据的类型
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置最终输出数据的类型
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        //设置排序类，为自定义排序
        job.setSortComparatorClass(Sort.class);
        //设置输入输出数据的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
