package site.xixing.reverseIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/30 14:45
 */
public class ReverseIndexOneMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text k=new Text();
    IntWritable v=new IntWritable(1);
    String name;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit= (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        String[] words = line.split(" ");
        for(String word:words){
            String str=word+"--"+name;
            k.set(str);
            context.write(k,v);
        }
    }
}
