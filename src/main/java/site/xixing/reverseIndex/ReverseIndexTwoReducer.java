package site.xixing.reverseIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/30 15:23
 */
public class ReverseIndexTwoReducer extends Reducer<Text,Text,Text,Text> {

    Text k=new Text();
    Text v=new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //String str="";
        StringBuffer stringBuffer=new StringBuffer();
        for(Text value:values){
            //str+=value.toString()+"\t";
            stringBuffer.append(value.toString());
        }
        k.set(key);
        v.set(stringBuffer.toString());
        context.write(k,v);
    }
}
