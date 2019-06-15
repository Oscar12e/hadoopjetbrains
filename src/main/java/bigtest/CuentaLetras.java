package bigtest;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.File;


public class CuentaLetras extends Configured implements Tool { 
	
	public static void main(String[] args) throws Exception {
		System.out.println("Working Directory = " +
				System.getProperty("user.dir") + "/output");
		int res = ToolRunner.run(new CuentaLetras(), args);
		System.out.println("Res ONe: " + res);
		System.out.println("Working Directory = " +
				System.getProperty("user.dir") + "/output");

		File f = new File("/user/cloudera/Oskr-erick/actions-logs.json");

		System.out.println("The path exits: " + f.exists());
	    System.exit(res);
	}

	public int run(String[] args) throws Exception {
	    Job job = Job.getInstance(getConf(), "Contando Letras");
	    job.setJarByClass(this.getClass());
		System.out.println("Working Directory = " +
				System.getProperty("user.dir"));

		//String outputDir = System.getProperty("user.dir") + "/output/new";
		String outputDir = "/user/cloudera/Oskr-erick/output";


	    //System.out.println(args[0]);
	    // Use TextInputFormat, the default unless job.setInputFormatClass is used
	    //FileInputFormat.addInputPath(job, new Path("resources/small-log.json"));
		FileInputFormat.addInputPath(job, new Path("/user/cloudera/Oskr-erick/actions-logs.json"));
		//System.out.println(args[0]);
	    FileOutputFormat.setOutputPath(job, new Path(outputDir));
		//System.out.println(args[0]);
	    job.setMapperClass(LetrasMaper.class);
	    job.setReducerClass(LetrasReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
		System.out.println("Ok so, wanna work now?");
	    return job.waitForCompletion(true) ? 0 : 1;
	}

}
