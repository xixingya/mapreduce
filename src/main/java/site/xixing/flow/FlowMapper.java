package site.xixing.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/6 15:15
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    Text k=new Text();
    FlowBean flowBean=new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        //1.获取一行
        String line = value.toString();
        //2.切割，获取相应的数据
        String[] fields = line.split("\t");
        //手机号
        String phone =fields[1];
        //上行流量
        long upflow=Long.parseLong(fields[fields.length-3]);
        //下行流量
        long downflow=Long.parseLong(fields[fields.length-2]);
        //封装对象
        flowBean.set(upflow,downflow);

        k.set(phone);
        context.write(k,flowBean);

    }
}
