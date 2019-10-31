package site.xixing.sharedfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/31 14:32
 */
public class SharedFriendsOneReducer extends Reducer<Text,Text,Text,Text> {
    Text k=new Text();
    Text v=new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for(Text person:values){
            stringBuffer.append(person.toString()+",");
        }
        v.set(stringBuffer.toString());
        context.write(key,v);
    }
}

