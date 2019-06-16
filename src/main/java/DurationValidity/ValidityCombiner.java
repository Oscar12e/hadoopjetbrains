package DurationValidity;

import bigtest.LetrasMaper;
import bigtest.LetrasReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ValidityCombiner extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Validity Analizer - oskr_erick");
        job.setJarByClass(this.getClass());

        //Args -- Para la revisión toca usar este
        String inputDir = args[0];
        String outputDir = args[1];

        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setMapperClass(ValidityMapper.class);
        job.setReducerClass(ValidityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("Ok so, wanna work now?");
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
