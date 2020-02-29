package com.fashion.storm.reliable.topology;

import com.fashion.storm.reliable.bolt.SentenceSplitBolt;
import com.fashion.storm.reliable.bolt.WordCountBolt;
import com.fashion.storm.reliable.spout.ReliableSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 出现超时，或者显式调用collector.fail方法,
 */
public class ReliableWordCountTopology  {

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("spout_id",new ReliableSentenceSpout());

        topologyBuilder.setBolt("split_id",new SentenceSplitBolt()).shuffleGrouping("spout_id");

        topologyBuilder.setBolt("wordCount_id",new WordCountBolt()).fieldsGrouping("split_id",new Fields("word"));

        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("word_count",config,topologyBuilder.createTopology());

    }
}
