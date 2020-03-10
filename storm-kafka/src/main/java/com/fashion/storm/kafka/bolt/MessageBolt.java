package com.fashion.storm.kafka.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Objects;

public class MessageBolt extends BaseRichBolt  {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        String word = input.getStringByField("word");

        if(Objects.equals(word,"yangyuguang30")){
            System.out.println("message:" + word  +"执行失败,fail");
            collector.fail(input);
        }else{
            System.out.println("message:" + word  +"执行成功,ack");
            collector.ack(input);
        }

    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
