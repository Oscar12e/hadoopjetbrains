/*

 */

import durationvalidity.InteractionCombiner;
import bigtest.CuentaLetras;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import potentialmarket.InterestCombiner;
import valuablevalidity.ValueCombiner;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Working Directory = " +
                System.getProperty("user.dir") + "/output");
        Tool tool = null;

        if (args[3].equals("-t")) //Es el tiempo valido?
            tool = new InteractionCombiner();
        else if (args[3].equals("-p")) //Mercado potencial
            tool = new InterestCombiner();
        else if (args[3].equals("-v")) //Mercado potencial
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
