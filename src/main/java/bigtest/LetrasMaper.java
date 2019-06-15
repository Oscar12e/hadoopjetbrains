package bigtest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;

public class LetrasMaper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context)
            throws IOException, InterruptedException
    {
        try{
            JSONParser parser = new JSONParser();
            JSONArray array =  (JSONArray)parser.parse(lineText.toString());
            String animal;
            String line = lineText.toString();
            String[] tuple = line.split("\\n");



            for(int i=0;i<50; i++){
                JSONObject obj = (JSONObject) array.get(i);//new JSONObject(tuple[i]);

                animal = (String) obj.get("time");

                //animal = embedded.getString("section");
                context.write(new Text(animal), one);
            }
        }catch(ParseException e){
            e.printStackTrace();
        } catch (Exception e){
            System.out.println(e.toString());
        }

    }
}