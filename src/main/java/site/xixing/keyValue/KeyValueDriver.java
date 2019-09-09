package site.xixing.keyValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/9 15:38
 */
public class KeyValueDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job=Job.getInstance(configuration);

        job.setJarByClass(KeyValueDriver.class);
        job.setMapperClass(KeyValueMapper.class);
        job.setReducerClass(KeyValueReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);










    }
}
