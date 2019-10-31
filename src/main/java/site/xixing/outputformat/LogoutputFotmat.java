package site.xixing.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/9 16:55
 */
public class LogoutputFotmat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        RecordWriter<Text,NullWritable> recordWriter=new LogRecordWriter(taskAttemptContext);

        return recordWriter;
    }
}
