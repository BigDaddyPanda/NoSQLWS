package mapreduceboi;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author panda_pc
 */
public class MapReduceBoi {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        System.out.println("Configuration=>Done");
//        Job job = Job.getInstance(conf, "word count");
//        System.out.println("Job=>Done");
//
//        job.setJarByClass(MapReduceBoi.class);
//        System.out.println("setJarByClass=>Done");
//        job.setMapperClass(TokenizerMapper.class);
//        System.out.println("setMapperClass=>Done");
//        job.setCombinerClass(IntSumReducer.class);
//        System.out.println("setCombinerClass=>Done");
//        job.setReducerClass(IntSumReducer.class);
//        System.out.println("setReducerClass=>Done");
//        job.setOutputKeyClass(Text.class);
//        System.out.println("setOutputKeyClass=>Done");
//        job.setOutputValueClass(IntWritable.class);
//        System.out.println("setOutputValueClass=>Done");
//        FileInputFormat.addInputPath(job, new Path("/home/panda_pc/NetBeansProjects/MapReduceBoi/src/mapreduceboi/input.txt"));
//        System.out.println("addInputPath=>Done");
//        FileOutputFormat.setOutputPath(job, new Path("/home/panda_pc/NetBeansProjects/MapReduceBoi/src/mapreduceboi/output.txt"));
//        System.out.println("setOutputPath=>Done");
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
////        System.out.println(job.waitForCompletion(true) ?"Done":"Not Yet");
//    }
}
