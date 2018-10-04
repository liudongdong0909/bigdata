package com.bigdata.example.flow.sum;

import com.bigdata.example.flow.bean.FlowBean;
import com.bigdata.example.flow.combiner.FlowCountCombiner;
import com.bigdata.example.flow.partitioner.ProvincePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 运营商流量统计
 * <p>
 * 每一手机号的总流量
 *
 * @author donggua
 * @version 1.0
 * @create 2018/10/1
 */
public class FlowCount extends Configured implements Tool {


    public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        Text outputKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
            String line = value.toString();
            String[] wordValues = line.split("\t");

            outputKey.set(wordValues[1]);
            long upFlow = Long.parseLong(wordValues[wordValues.length - 3]);
            long dfFlow = Long.parseLong(wordValues[wordValues.length - 2]);

            context.write(outputKey, new FlowBean(upFlow, dfFlow));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

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


    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        // job.setJar("/home/hadoop/wc.jar");
        job.setJarByClass(this.getClass());

        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 一下两句在 使用自定义分区器的时候使用
        // 定义我们自定义的数据分区器
        job.setPartitionerClass(ProvincePartitioner.class);
        // 同时指定相应"分区"数量的reduceTask
        job.setNumReduceTasks(5);

        // 设置combiner - 慎用
        // job.setCombinerClass(FlowCountCombiner.class);
        // job.setCombinerClass(FlowCountReducer.class);

        /**
         * 如果不设置 inputFormat， 默认 TextFileInputFormat
         */
        job.setInputFormatClass(CombineFileInputFormat.class);
        CombineFileInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineFileInputFormat.setMinInputSplitSize(job, 2097152);

        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int status = ToolRunner.run(configuration, new FlowCount(), args);
        System.exit(status);
    }
}
