package site.xixing.reverseIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/30 15:18
 */
public class ReverseIndexTwoMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text k=new Text();
    Text v=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("--");
        String[] temp = words[1].split("\t");
        System.out.println(temp[0]);
        String str=temp[0]+"-->"+temp[1];
        k.set(words[0]);
        v.set(str);
        context.write(k,v);

    }
}
