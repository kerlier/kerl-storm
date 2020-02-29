package com.fashion.storm.reliable.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {


    private OutputCollector collector ;

    private HashMap<String,Long> counts = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts  =new  HashMap<String,Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");

        Long count = this.counts.get(word);

        if(count ==null){
            count =0l;
        }

        count++;
        this.counts.put(word,count);

        System.out.println("word: " + word + ", count: "+ count);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
