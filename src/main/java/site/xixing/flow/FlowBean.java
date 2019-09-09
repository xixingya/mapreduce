package site.xixing.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/6 15:06
 */
public class FlowBean implements Writable {

    private long upflow;
    private long downflow;
    private long sumflow;

    public FlowBean() {
        super();
    }

    public FlowBean(long upflow, long downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow=downflow+upflow;
    }
    public void set(long upflow, long downflow){
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow=downflow+upflow;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow + "\t" + sumflow + '\t';
    }

    /**
     *
     * @param //dataOutput框架传来的输出流
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumflow);

    }

    public void readFields(DataInput dataInput) throws IOException {
        upflow=dataInput.readLong();
        downflow=dataInput.readLong();
        sumflow=dataInput.readLong();
    }
}
