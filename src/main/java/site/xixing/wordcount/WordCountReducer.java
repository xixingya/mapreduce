package site.xixing.wordcount;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/5 11:17
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * （1）用户自定义的Reducer要继承自己的父类
 * 	（2）Reducer的输入数据类型对应Mapper的输出数据类型，也是KV
 * 	（3）Reducer的业务逻辑写在reduce()方法中
 * 	（4）Reducetask进程对每一组相同k的<k,v>组调用一次reduce()方法
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    /**
     *
     * @param key；单词
     * @param values：单词个数的集合  1
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        //1.累加
        int sum=0;
        for(IntWritable value:values){
            sum+=value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
