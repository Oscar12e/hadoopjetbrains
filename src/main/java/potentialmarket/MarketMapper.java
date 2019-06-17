package potentialmarket;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class MarketMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {


    public void map(LongWritable offset, Text lineText, Context context) {
        try{
            JSONObject entry = new JSONObject(lineText.toString());
            JSONObject action = entry.getJSONObject("action");
            if (action.has("utm")){
                String campaign = action.getJSONObject("utm").getString("campaign");
                float duration =  (float) action.getDouble("duration");
                context.write(new Text(campaign + ","), new FloatWritable(duration));
            }
        } catch (Exception e){
            System.out.println(e.toString());
        }

    }

}
