package bigtest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;


import java.io.IOException;

public class LetrasMaper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException
    {
        try{
            JSONArray array =  new JSONArray(lineText.toString());

            String animal;

            for(int i=0;i<array.length(); i++){
                JSONObject obj = array.getJSONObject(i);//new JSONObject(tuple[i]);
                animal = obj.getString("gender");

                context.write(new Text(animal), one);
            }
        } catch (Exception e){
            System.out.println(e.toString());
        }

    }
}