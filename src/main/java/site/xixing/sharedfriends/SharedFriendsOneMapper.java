package site.xixing.sharedfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/31 14:27
 */
public class SharedFriendsOneMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text k=new Text();
    Text v=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split(":");
        System.out.println(splits.length);
        //System.out.println(splits[0]);
        String[] words = splits[1].split(",");
        System.out.println(splits[1]);
        for(String word:words){
            k.set(word);
            v.set(splits[0]);
            context.write(k,v);

        }


    }
}
