package site.xixing.sharedfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import site.xixing.reverseIndex.ReverseIndexOneDriver;
import site.xixing.reverseIndex.ReverseIndexOneMapper;
import site.xixing.reverseIndex.ReverseIndexOneReducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/10/31 14:44
 */
public class SharedFriendOneDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SharedFriendOneDriver.class);
        job.setMapperClass(SharedFriendsOneMapper.class);
        job.setReducerClass(SharedFriendsOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
