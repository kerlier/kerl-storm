package com.fashion.storm.reliable.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ReliableSentenceSpout extends BaseRichSpout {

    //使用一个map来存放已经发射出去的tuple
    private ConcurrentHashMap<String, Values> pending =null;

    private SpoutOutputCollector collector;

    private String[] sentences = {"i love you","my name is tom"};

    private int index = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
            System.out.println("map"+ map.toString());
            this.pending= new ConcurrentHashMap<String, Values>();
    }

    public void nextTuple() {

        if(index  >= 2){
            return;
        }

        System.out.println("index:" +index);
        Values values = new Values(sentences[index]);

        String msgId = UUID.randomUUID().toString();

        this.pending.put(msgId,values);

        System.out.println("提交tuple:" + values.toString());

        this.collector.emit(values,msgId);

        index ++;

        System.out.println("length："+ sentences.length);

        try {
            Time.sleepSecs(5);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        //这里的declare跟上面的emit的参数个数不同
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    /**
     * 执行成功后会执行ack方法
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        System.out.println(this.pending.get(msgId )+"执行成功");
       this.pending.remove(msgId);
    }

    /**
     * 执行失败后执行fail方法
     * 失败的两种原因：
     *     第一种： 超时
     *     第二种： 显式调用collector.fail()方法
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        System.out.println(this.pending.get(msgId )+"执行失败,重新提交 msgId"+ this.pending.get(msgId).toString());
        this.collector.emit(this.pending.get(msgId),msgId);
    }
}
