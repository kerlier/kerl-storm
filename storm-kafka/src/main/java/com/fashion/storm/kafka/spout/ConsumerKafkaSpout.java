package com.fashion.storm.kafka.spout;

import com.fashion.storm.kafka.bolt.MessageBolt;
import com.fashion.storm.kafka.translator.MyRecordTranslator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.concurrent.TimeUnit;


public class ConsumerKafkaSpout {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //setRecordTranslator的作用就是对当前的record.value进行操作，new Fields()指的这个参数的名字
        KafkaSpoutConfig.Builder<String, String> builder = KafkaSpoutConfig.builder("192.168.5.134:9092","test-topic").setRecordTranslator(new MyRecordTranslator(),
                new Fields("word")).setRetry( new KafkaSpoutRetryExponentialBackoff(
                new KafkaSpoutRetryExponentialBackoff.TimeInterval(500L, TimeUnit.MICROSECONDS),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2),
                3,
                KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10)
        ));
        // setRetry 设置失败的重试机制


        builder.setGroupId("test_storm_wc");

        KafkaSpoutConfig<String, String> kafkaSpoutConfig = builder.build();
        topologyBuilder.setSpout("WordCountFileSpout",new KafkaSpout<String,String>(kafkaSpoutConfig), 1);
        topologyBuilder.setBolt("consumerBolt",new MessageBolt(),1).shuffleGrouping("WordCountFileSpout");

        // 集群运行
        if(null!=args && args.length>0){
            Config config = new Config();
            config.setNumWorkers(1);
            config.setDebug(true);
            StormSubmitter.submitTopology("teststorm", config, topologyBuilder.createTopology());
        }else{
            //本地测试
            Config config = new Config();
            config.setNumWorkers(1);
            config.setDebug(true);
            config.setMaxTaskParallelism(5);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("teststorm", config, topologyBuilder.createTopology());
            Utils.sleep(60000);
            // 执行完毕，关闭cluster
            cluster.shutdown();
        }
    }
}
