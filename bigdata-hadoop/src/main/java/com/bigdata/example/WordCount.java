package com.bigdata.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 第一个案例
 *
 * @author donggua
 * @version 1.0
 * @create 2018/9/28
 */
public class WordCount {

    /**
     * step 1: mapper
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapOutputKey = new Text();

        private final static IntWritable mapOutputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String lineValue = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
            while (stringTokenizer.hasMoreTokens()) {
                String wordValue = stringTokenizer.nextToken();
                mapOutputKey.set(wordValue);
                context.write(mapOutputKey, mapOutputValue);
            }

        }
    }


    /**
     * step 2: reduce
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputValue = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            outputValue.set(sum);

            context.write(key, outputValue);
        }
    }


    /**
     * step 3: driver
     */
    public int run(String[] args) throws Exception {
        // 3.1 get configuration
        Configuration configuration = new Configuration();
        // configuration.set("mapreduce.framework.name", "local");
       // configuration.set("fs.defaultFS", "file:///");
        // 3.2 create job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());

        // 3.3 run jar
        // job.setJarByClass(WordCount.class);
        job.setJar("/Users/donggua/IdeaProjects/bigdata/bigdata-hadoop/target/bigdata-hadoop.jar"); // 本地运行集群测试 - 真TMD奇葩 这个路径

        // 3.4 set job
        // input -> map -> reduce -> output
        // 3.4.1 input
        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputPath);

        // 3.4.2 map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 3.4.3 reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // job.setInputFormatClass(CombineFileInputFormat.class);
        // CombineFileInputFormat.setMaxInputSplitSize(job, 4194304);
        // CombineFileInputFormat.setMinInputSplitSize(job, 2097152);

        // 3.4.4 output
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 3.5 commit job
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        WordCount wordCount = new WordCount();
        int result = wordCount.run(args);
        System.exit(result);
    }
}
