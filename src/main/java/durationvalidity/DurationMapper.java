package durationvalidity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class DurationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context) {
        try{
            JSONObject entry = new JSONObject(lineText.toString());//new JSONObject(tuple[i]);
            JSONObject action = entry.getJSONObject("action");
            if (action.has("utm")){
                String date = entry.getString("time").split(" ")[0];
                String campaign = action.getJSONObject("utm").getString("campaign");
                context.write(new Text(campaign + ","+ date+","), one);
            }
        } catch (Exception e){
            System.out.println(e.toString());
        }

    }

}
