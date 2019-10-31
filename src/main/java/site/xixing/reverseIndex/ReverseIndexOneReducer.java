package site.xixing.reverseIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/30 14:54
 */
public class ReverseIndexOneReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    Text k=new Text();
    IntWritable v=new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable value:values){
            sum+=value.get();
        }
        k.set(key);
        v.set(sum);
        context.write(k,v);

    }
}
