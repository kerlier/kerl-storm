package com.fashion.storm.topology;

import com.fashion.storm.bolt.ReportBolt;
import com.fashion.storm.bolt.SentenceSpout;
import com.fashion.storm.bolt.SentenceSplitBolt;
import com.fashion.storm.bolt.WordCountBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

import java.util.Map;


public class WordCountTopology  {

    private static final String SENTENCE_SPOUT_ID= "sentence_spout_id";
    private static final String SENTENCE_SPLIT_ID ="sentence_split_id";
    private static final String WORD_COUNT_ID= "word_count_id";
    private static final String  REPORT_ID= "report_id";


    /**
     * fieldsGrouping 保证word字段值相同的tuple会路由到同一个wordCountBolt实例中
     * globalGrouping wordCountBolt发射出来的tuple会路由到唯一的reportBolt实例中
     * @param args
     */
    public static void main(String[] args) {

        SentenceSpout sentenceSpout = new SentenceSpout();
        SentenceSplitBolt sentenceSplitBolt = new SentenceSplitBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(SENTENCE_SPOUT_ID,sentenceSpout);
        topologyBuilder.setBolt(SENTENCE_SPLIT_ID,sentenceSplitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        topologyBuilder.setBolt(WORD_COUNT_ID,wordCountBolt).fieldsGrouping(SENTENCE_SPLIT_ID,new Fields("word"));
        topologyBuilder.setBolt(REPORT_ID,reportBolt).globalGrouping(WORD_COUNT_ID);

        Config config = new Config();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("word-count",config,topologyBuilder.createTopology());

        try {
            Time.sleepSecs(10);
        }catch (Exception e
        ){
            e.printStackTrace();
        }

        localCluster.killTopology("word-count");
        localCluster.shutdown();

    }
}
