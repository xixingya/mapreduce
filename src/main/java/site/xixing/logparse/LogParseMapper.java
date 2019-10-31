package site.xixing.logparse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/16 15:19
 */
public class LogParseMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text k=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Boolean result=LogParse(line,context);
        if(!result){
            return;
        }
        k.set(line);
        context.write(k,NullWritable.get());

    }

    private Boolean LogParse(String line, Context context) {
        String[] fields = line.split(" ");
        if(fields.length>11){
            context.getCounter("map","log>11").increment(1);
            return true;
        }else {
            context.getCounter("map","log<=11").increment(1);

            return false;
        }
    }
}
