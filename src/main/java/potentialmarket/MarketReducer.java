package potentialmarket;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MarketReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text word, Iterable<FloatWritable> counts, Context context)
            throws IOException, InterruptedException {

        float sum = 0;
        for (FloatWritable count : counts) {
            sum += count.get();
        }
        context.write(word, new FloatWritable(sum));
    }
}
