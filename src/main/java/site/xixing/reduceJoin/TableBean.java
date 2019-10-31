package site.xixing.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/10 9:41
 */
public class TableBean implements Writable {

    private String orderId;
    private String pid;
    private int amount;
    private String pname;
    private String flag;

    public TableBean() {
    }

    @Override
    public String toString() {
        return orderId + "\t" + pname + "\t" +amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);

    }

    public void readFields(DataInput dataInput) throws IOException {
        orderId=dataInput.readUTF();
        pid=dataInput.readUTF();
        amount=dataInput.readInt();
        pname=dataInput.readUTF();
        flag=dataInput.readUTF();
    }
}
