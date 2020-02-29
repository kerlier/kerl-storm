package com.fashion.storm.bolt;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * @author  yang
 */
public class SentenceSpout extends BaseRichSpout {


    private SpoutOutputCollector collector;
    private String [] sentences = {"i love you", "i need you"};
    //先定义collector对象

    private int index = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        //一直发送
        this.collector.emit(new Values(sentences[index]));

        index++;

        if(index >= sentences.length){
            index = 0;
        }

        try {
            Time.sleep(1000);
        }catch (Exception e){
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
