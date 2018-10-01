package com.bigdata.template;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 最标准的 mapreduce 代码模板
 *
 * @author donggua
 * @version 1.0
 * @create 2018/9/29
 */
public class TemplateMapReduce extends Configured implements Tool {

    public static class TemplateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // NOTHING
            // super.setup(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // NOTHING
            // super.cleanup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // TODO

        }
    }

    public static class TemplateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // NOTHING
            // super.setup(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // NOTHING
            // super.cleanup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // TODO
        }
    }


    @Override
    public int run(String[] strings) throws Exception {

        Configuration configuration = super.getConf();

        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Path inputPath = new Path(strings[0]);
        FileInputFormat.addInputPath(job, inputPath);

        job.setMapperClass(TemplateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TemplateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(strings[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        TemplateMapReduce wordCountMapReduce = new TemplateMapReduce();
        int result = wordCountMapReduce.run(args);
        System.exit(result);
    }
}
