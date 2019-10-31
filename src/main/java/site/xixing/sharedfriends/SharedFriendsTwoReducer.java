package site.xixing.sharedfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/31 15:14
 */
public class SharedFriendsTwoReducer extends Reducer<Text, Text,Text,Text> {

    Text v=new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();

        for(Text value:values){
            stringBuffer.append(value.toString()+"\t");
        }
        v.set(stringBuffer.toString());
        context.write(key,v);
    }
}
