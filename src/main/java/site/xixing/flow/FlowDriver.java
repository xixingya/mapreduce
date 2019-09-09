package site.xixing.flow;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/6 15:32
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);
        //2.设置jar包
        job.setJarByClass(FlowDriver.class);
        //3.设置关联的包
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //设置mappper输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //设置最终输出数据的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置输入输出数据的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
