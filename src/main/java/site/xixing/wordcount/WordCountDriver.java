package site.xixing.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/6 8:01
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job对象
        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration);
        //2.指定jar包路径
        job.setJarByClass(WordCountDriver.class);
        //3.关联mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.指定输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //6.指定job输入数据的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //7.指定job输出数据的路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //8.提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
