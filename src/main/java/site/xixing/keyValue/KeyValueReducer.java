package site.xixing.keyValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/9 15:34
 */
public class KeyValueReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        int sum=0;
        for(IntWritable value:values){
            sum+=value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
