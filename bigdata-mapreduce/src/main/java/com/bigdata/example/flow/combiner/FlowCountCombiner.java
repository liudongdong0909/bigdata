package com.bigdata.example.flow.combiner;

import com.bigdata.example.flow.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * $$
 * <p>
 * combiner 其实就是一个reduce，
 * 但是在使用的时候一定要慎重。
 * <p>
 * 1. 对任务的执行次数是有要求的
 * 2. combiner 不能影响正常的业务
 *
 * @author donggua
 * @version 1.0
 * @create 2018/10/2
 */
public class FlowCountCombiner extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outputValue;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //<183323,bean1><183323,bean2><183323,bean3><183323,bean4>.......
        long sum_upFlow = 0;
        long sum_dfFlow = 0;

        for (FlowBean flowBean : values) {
            sum_upFlow += flowBean.getUpFlow();
            sum_dfFlow += flowBean.getdFlow();
        }

        outputValue = new FlowBean(sum_upFlow, sum_dfFlow);
        context.write(key, outputValue);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
