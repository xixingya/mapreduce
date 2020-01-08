package site.xixing.douban;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2020/1/5 9:54
 */
public class DoubanMapper extends Mapper<LongWritable, Text, DoubleWritable,Text> {
    DoubleWritable k=new DoubleWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String book = value.toString();
        String[] values = book.split(",");
        if (values.length>=2) {
            k.set(Double.parseDouble(values[1]));
            context.write(k, value);
        }
    }
}
