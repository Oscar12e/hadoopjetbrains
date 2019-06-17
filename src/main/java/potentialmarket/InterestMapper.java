package potentialmarket;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

public class InterestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context) {
        try{
            JSONArray array =  new JSONArray(lineText.toString());
            JSONObject action;
            String campaign;
            int userId;


            for(int i=0;i<array.length(); i++){
                JSONObject entry = array.getJSONObject(i);//new JSONObject(tuple[i]);
                action = entry.getJSONObject("action");
                if (!action.has("utm"))
                    continue;
                userId = entry.getInt("user_id");
                campaign = action.getJSONObject("utm").getString("campaign");
                context.write(new Text(campaign + "," + userId + ","), one);
            }
        } catch (Exception e){
            System.out.println(e.toString());
        }

    }

}
