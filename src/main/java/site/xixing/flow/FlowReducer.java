package site.xixing.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/6 15:26
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    FlowBean flowBean=new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        //累加
        long sumUpFlow=0;
        long sumDownFlow=0;
        for(FlowBean value:values){
            sumUpFlow+=value.getUpflow();
            sumDownFlow+=value.getDownflow();
        }
        //封装对象
        flowBean.set(sumUpFlow,sumDownFlow);
        //传出去
        context.write(key,flowBean);

    }
}
