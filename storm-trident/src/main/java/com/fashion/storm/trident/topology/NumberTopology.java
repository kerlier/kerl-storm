package com.fashion.storm.trident.topology;

import com.fashion.storm.trident.function.NumberFunction;
import com.fashion.storm.trident.operation.*;
import com.fashion.storm.trident.spout.DiagnosisEventSpout;
import com.fashion.storm.trident.spout.NumberSpout;
import com.fashion.storm.trident.state.OutbreakTrendFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class NumberTopology {

    public static void main(String[] args) {

        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("cdc",config,buildTopology());

        try {
            Thread.sleep(20000);
        }catch (Exception e){
            e.printStackTrace();
        }

        localCluster.shutdown();
    }


    public static StormTopology buildTopology(){

        TridentTopology tridentTopology = new TridentTopology();

        NumberSpout spout = new NumberSpout();

        Stream inputStream = tridentTopology.newStream("number", spout);

        inputStream.each(new Fields("age2"), new NumberFunction(), new Fields("age4"));

        return  tridentTopology.build();
    }
}
