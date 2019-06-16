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
            String description, interaction, campaign;

            String animal;

            for(int i=0;i<array.length(); i++){
                JSONObject entry = array.getJSONObject(i);//new JSONObject(tuple[i]);
                action = entry.getJSONObject("action");
                if (!action.has("utm"))
                    continue;

                campaign = action.getJSONObject("utm").getString("campaign");
                description = action.getString("description");
                interaction = description.equals("like")? "like": "content";

                animal = campaign + "," + interaction + "," ;
                context.write(new Text(animal), one);
            }
        } catch (Exception e){
            System.out.println(e.toString());
        }

    }

}
