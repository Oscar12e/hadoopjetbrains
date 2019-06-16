package potentialmarket;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class InterestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable count : counts) {
            sum += count.get();
        }
        context.write(word, new IntWritable(sum));
    }
}
