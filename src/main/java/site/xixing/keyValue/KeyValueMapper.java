package site.xixing.keyValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/9 15:30
 */
public class KeyValueMapper extends Mapper<Text,Text,Text, IntWritable> {

    IntWritable v=new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //super.map(key, value, context);
        context.write(key,v);

    }
}
