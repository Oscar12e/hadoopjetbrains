package bigtest;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class LetrasMaper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
	private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    
    		context.write(new Text(lineText.toString()),one);
    }
}
