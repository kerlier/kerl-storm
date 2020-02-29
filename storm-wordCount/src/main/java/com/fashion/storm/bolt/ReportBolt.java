package com.fashion.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * 这个bolt不需要发送数据，所以可以不用初始化collector
 * @author  yang
 */
public class ReportBolt extends BaseRichBolt {


    private Map<String ,Long > counts = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word,count);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {

        //在cleanup中输出所有的参数
        List<String> keys = new ArrayList<String>();

        keys.addAll(counts.keySet());

        Collections.sort(keys);

        for (String key: keys) {
            System.out.println("key : " + key +", count: "+  counts.get(key));
        }
    }
}
