package valuablevalidity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

public class ValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable offset, Text lineText, Context context) {
        try{
            JSONArray array =  new JSONArray(lineText.toString());
            JSONObject action;
            String description, interaction;

            for(int i=0;i<array.length(); i++){
                JSONObject entry = array.getJSONObject(i);//new JSONObject(tuple[i]);
                action = entry.getJSONObject("action");
                if (!action.has("utm"))
                    continue;

                description = action.getString("description");
                interaction = description.equals("like")? "like": "content";

                context.write(new Text(action.getJSONObject("utm").getString("campaign") + "," + interaction + ","), one);
            }
        } catch (Exception e){
            System.out.println(e.toString());
        }

    }

}
