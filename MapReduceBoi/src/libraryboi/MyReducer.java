/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package libraryboi;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author panda_pc
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable value = new IntWritable(0);

//    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        value.set(sum);
        context.write(key, value);
    }
}
