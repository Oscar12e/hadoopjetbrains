package bigtest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;


import java.io.IOException;

public class LetrasMaper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    //private int total = 0;
    //private IntWritable two = new IntWritable(total);

    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException
    {
        try{
            JSONArray array =  new JSONArray(lineText.toString());

            String post;
            String tag;
            String action;
            String tagg;


            for(int i=0;i<array.length(); i++){
                JSONObject obj = array.getJSONObject(i);//new JSONObject(tuple[i]);
                action = obj.getString("action");
                //System.out.println(action);
                JSONObject jsonObject = new JSONObject(action);
                //System.out.println(jsonObject.toString());
                    try{
                    post = jsonObject.getString("post");
                    System.out.println(post);
                    JSONObject jsonObject2 = new JSONObject(post);
                    tag = jsonObject2.getString("tags");
                    System.out.println(tag);
                    JSONArray array2 = new JSONArray(tag);
                    for (int z = 0; z < array2.length(); z++) {
                        JSONObject jsonObjtag = array2.getJSONObject(z);
                        //System.out.println(jsonObjtag.toString());
                        tagg = jsonObjtag.getString("_id");
                     //   total++;

                        context.write(new Text(tagg), one);
                    }

                }
                    catch (Exception f){
                        System.out.println(f.toString());
                    }


                //context.write(new Text("info"), two);

            }



        } catch (Exception e){
            System.out.println(e.toString());
        }

    }
}