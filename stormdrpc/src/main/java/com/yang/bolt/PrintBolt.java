package com.yang.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PrintBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        //第一个应该是requestId
        Long requestId = tuple.getLong(0);
        System.out.println("requestId" + requestId);
        String tupleString = tuple.getString(1);
        System.out.println("第一个参数：" + tupleString);
        basicOutputCollector.emit(new Values(requestId, tupleString + "======"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "result"));
    }
}
