package potentialmarket;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class InterestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context) {
        try{
            JSONObject entry = new JSONObject(lineText.toString());
            JSONObject action = entry.getJSONObject("action");
            if (action.has("utm")){
                int userId = entry.getInt("user_id");
                String campaign = action.getJSONObject("utm").getString("campaign");
                context.write(new Text(campaign + "," + userId + ","), one);
            }
        } catch (Exception e){
            System.out.println(e.toString());
        }

    }

}
