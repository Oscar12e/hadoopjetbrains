package bigtest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.SaslOutputStream;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class LetrasMaper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException {
        String animal;
        String line = lineText.toString();
        String[] tuple = line.split("\\n");
        try{
            for(int i=0;i<tuple.length; i++){
                System.out.println(tuple[i]);
                JSONObject obj = new JSONObject(tuple[i]);
                animal = obj.getString("animal");
                context.write(new Text(animal), one);
            }
        }catch(JSONException e){
            e.printStackTrace();
        }

    }
}