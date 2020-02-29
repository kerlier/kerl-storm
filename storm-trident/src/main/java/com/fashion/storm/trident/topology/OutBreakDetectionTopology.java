package com.fashion.storm.trident.topology;

import com.fashion.storm.trident.operation.*;
import com.fashion.storm.trident.spout.DiagnosisEventSpout;
import com.fashion.storm.trident.state.OutbreakTrendFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

import java.lang.reflect.Field;

/**
 * Detection 侦查
 */
public class OutBreakDetectionTopology {

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

        DiagnosisEventSpout spout = new DiagnosisEventSpout();

        Stream inputStream = tridentTopology.newStream("event", spout);


        //一个stream中可能有很多的字段，所以第一个field中指定了输入集合
        //each 方法 : 第一个field是输入集合，中间的是operation,后面是这个operation新加的字段
        inputStream.each(new Fields("event"),new DiseaseFilter())
                 //这一行表示对每个tuple都进行cityAssignment的操作，第一个new Field(Event)，
                 //表示cityAssignment会对tuple中的event字段进行操作,然后生成一个新的字段值
                 //叫做city,然后附在tuple上,向后发送
                .each(new Fields("event"),new CityAssignment(),new Fields("city"))
                .each(new Fields("event","city"),new HourAssignment(),new Fields("hour","cityDiseaseHour"))
                .groupBy(new Fields("cityDiseaseHour"))
                .persistentAggregate(new OutbreakTrendFactory(),new Count(),new Fields("count"))
                .newValuesStream()
                .each(new Fields("cityDiseaseHour","count"),new OutBreakDetector(),new Fields("alert"))
                .each(new Fields("alert"),new DispatchAlert(),new Fields());

        return  tridentTopology.build();
    }

}
