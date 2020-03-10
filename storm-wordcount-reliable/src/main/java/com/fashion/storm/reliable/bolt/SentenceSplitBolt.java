package com.fashion.storm.reliable.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SentenceSplitBolt extends BaseRichBolt {

    private  OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {

        String sentence = tuple.getStringByField("sentence");

        String[] words = sentence.split(" ");

        for (String word: words) {
            if(word.equals("name")) {
                this.collector.fail(tuple);
                return ;
            }
        }

        for (String word: words) {
                //为了有保障的处理，这里需要进行锚定之前的tuple, 锚定之前的tuple就是将之前的tuple作为参数传入到emit方法中
                this.collector.emit(tuple,new Values(word));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //这里的declare跟之前一致
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
