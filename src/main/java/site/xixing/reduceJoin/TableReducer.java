package site.xixing.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xixing
 * @version 1.0
 * @date 2019/9/10 10:10
 */
public class TableReducer extends Reducer<Text,TableBean, NullWritable,TableBean> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        //用来接收order的数据
        List<TableBean> orderBeans=new ArrayList<TableBean>();
        //用来接收pd表的数据
        TableBean pdBean=new TableBean();
        for(TableBean value:values){
            if(value.getFlag().equals("0")){
                //value是临时变量不能直接add进入。add的是value的地址
                // 不能按这样写：orderBeans.add(value);
                TableBean oBean= new TableBean();
                try {
                    BeanUtils.copyProperties(oBean,value);
                    orderBeans.add(oBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }else {
                //pd表
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

        }
        for(TableBean orderBean:orderBeans){
            orderBean.setPname(pdBean.getPname());
            context.write(NullWritable.get(),orderBean);
        }


    }
}
