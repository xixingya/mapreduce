package site.xixing.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/9 17:00
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream xixingOut = null;
    FSDataOutputStream othersOut = null;
    FileSystem fileSystem=null;

    public LogRecordWriter(TaskAttemptContext taskAttemptContext) {

        Configuration conf = new Configuration();
        try {
            //获取客户端
            fileSystem = FileSystem.get(conf);
            Path xixingPath = new Path("D:/xixing.txt");
            Path othersPath = new Path("D:/others.txt");
            //输出流
            xixingOut = fileSystem.create(xixingPath);
            othersOut = fileSystem.create(othersPath);


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String line = text.toString();
        //判断是否包含xixing
        if(line.contains("xixing")){
            xixingOut.write(line.getBytes());
        }else{
            othersOut.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(xixingOut);
        IOUtils.closeStream(othersOut);
        fileSystem.close();
    }
}
