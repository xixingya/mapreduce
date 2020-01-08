package site.xixing.douban;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2020/1/5 9:53
 */
public class DoubanReducer extends Reducer<DoubleWritable,Text,NullWritable,Text> {
    @Override

    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values){
            Text v=new Text(value.toString()+"\n");
            context.write(NullWritable.get(),value);
        }
    }
}
