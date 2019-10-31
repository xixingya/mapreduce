package site.xixing.sharedfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/31 15:05
 */
public class SharedFriendsTwoMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text k=new Text();
    Text v=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println(line);
        String[] friends2 = line.split("\t");
        //System.out.println(friends2[0]);
        String[] people = friends2[1].split(",");
        Arrays.sort(people);

        for(int i=0;i<people.length-1;i++){
            for(int j=i+1;j<people.length;j++){
                String str=people[i]+"-"+people[j];
                k.set(str);
                v.set(friends2[0]);
                context.write(k,v);
            }
        }



    }
}
