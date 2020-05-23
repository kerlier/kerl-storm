package com.fashion.storm.kafka.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class MessageBolt extends BaseRichBolt  {

    private static Logger LOGGER = LoggerFactory.getLogger(MessageBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        System.out.println("current word value is " + word);
//        try {
//            TimeUnit.MINUTES.sleep(2);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        if(Objects.equals(word,"yangyuguang30")){
            LOGGER.error("message"+word+ " 执行失败,fail");
            System.err.println("message"+word+ " 执行失败,fail");
            collector.fail(input);
        }else{
            LOGGER.info("message"+word+ " 执行成功,ack");
            collector.ack(input);
        }

    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
