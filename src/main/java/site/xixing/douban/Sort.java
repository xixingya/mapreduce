package site.xixing.douban;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author xixing
 * @version 1.0
 * @date 2020/1/7 16:58
 */
public class Sort extends WritableComparator {
    public Sort(){
        super(DoubleWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -a.compareTo(b);
    }
}
