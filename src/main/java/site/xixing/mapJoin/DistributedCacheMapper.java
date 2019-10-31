package site.xixing.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/11 10:14
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    HashMap<String,String> map=new HashMap<String, String>();
    Text k=new Text();

    /**
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获取输入字节流
        FileInputStream fis=new FileInputStream("D:\\test\\testJoin\\pd.txt");
        //获取转换流
        InputStreamReader isr=new InputStreamReader(fis,"UTF-8");
        //获取缓存流
        BufferedReader br=new BufferedReader(isr);
        String line=null;
        while(StringUtils.isNotEmpty(line=br.readLine())){
            String[] fields = line.split("\t");
            map.put(fields[0],fields[1]);
        }
        fis.close();
        isr.close();
        br.close();





    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //通过split切割，通过pid获取pname
        String[] fields = line.split("\t");
        String pid=fields[1];
        String pname=map.get(pid);
        String str=line+"\t"+pname;
        k.set(str);
        context.write(k,NullWritable.get());

    }
}
