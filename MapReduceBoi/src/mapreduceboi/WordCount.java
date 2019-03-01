/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mapreduceboi;

/**
 *
 * @author panda_pc
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable value = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            value.set(sum);
            context.write(key, value);
        }
    }
//
//    public static void main(String[] args) throws Exception {
//        
//        Configuration conf = new Configuration();
//        Job job = new Job(conf, "wordcount2");
//        System.out.println("Creating Job=>Done");
//        job.setJarByClass(WordCount.class);
//        System.out.println("Assigning Class=>Done");
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        job.setMapperClass(Map.class);
//        job.setReducerClass(Reduce.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setNumReduceTasks(1);
//        FileInputFormat.addInputPath(job, new Path("/home/panda_pc/NetBeansProjects/MapReduceBoi/src/mapreduceboi/input.txt"));
//        System.out.println("addInputPath=>Done");
//        FileOutputFormat.setOutputPath(job, new Path("/home/panda_pc/NetBeansProjects/MapReduceBoi/src/mapreduceboi/output.txt"));
//        System.out.println("setOutputPath=>Done");
//        boolean success = job.waitForCompletion(true);
//        System.out.println(success);
//    }
}
