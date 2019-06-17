/*

 */

import durationvalidity.DurationCombiner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import potentialmarket.MarketCombiner;
import valuablevalidity.ValueCombiner;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Working Directory = " +
                System.getProperty("user.dir") + "/output");
        Tool tool = null;

        if (args[2].equals("-d")) //Es el tiempo valido?
            tool = new DurationCombiner();
        else if (args[2].equals("-m")) //Mercado potencial
            tool = new MarketCombiner();
        else if (args[2].equals("-v")) //QUe es más valioso
            tool = new ValueCombiner();
        else{
            System.out.println("Non valid command.");
            return;
        }
        //inputs:
        //"/user/cloudera/Oskr-erick/small-log.json"
        //"/user/cloudera/Oskr-erick/actions-log.json"

        //ouputs:
        //"/user/cloudera/Oskr-erick/output/[something here]";

        int res = ToolRunner.run(tool, args);
        System.exit(res);
    }
}
