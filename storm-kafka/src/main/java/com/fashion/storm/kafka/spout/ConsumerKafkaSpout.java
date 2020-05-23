package com.fashion.storm.kafka.spout;

import com.fashion.storm.kafka.bolt.MessageBolt;
import com.fashion.storm.kafka.translator.MyRecordTranslator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ConsumerKafkaSpout {

	public static void main(String[] args)
			throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		// setRecordTranslator的作用就是对当前的record.value进行操作，new Fields()指的这个参数的名字
		KafkaSpoutConfig.Builder<String, String> builder = KafkaSpoutConfig.builder("localhost:9092", "test1")
//				.setProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
				.setOffsetCommitPeriodMs(1000)// 设置offset的提交时间
				.setRecordTranslator(new MyRecordTranslator(), new Fields("word"))
				.setRetry(new KafkaSpoutRetryExponentialBackoff(
						new KafkaSpoutRetryExponentialBackoff.TimeInterval(500L, TimeUnit.MICROSECONDS),
						KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), 3,
						KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10)));
		// setRetry 设置失败的重试机制

		builder.setGroupId("A");

		KafkaSpoutConfig<String, String> kafkaSpoutConfig = builder.build();
		topologyBuilder.setSpout("kafkaSpout", new KafkaSpout<String, String>(kafkaSpoutConfig), 2);
		topologyBuilder.setBolt("consumerBolt", new MessageBolt(), 3).shuffleGrouping("kafkaSpout");

		// 集群运行
		if (null != args && args.length > 0) {
			Map<String, Object> readDefaultConfig = Utils.readDefaultConfig();
			readDefaultConfig.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
			readDefaultConfig.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 3600);
			readDefaultConfig.put(Config.TOPOLOGY_WORKERS, 1);
			StormSubmitter.submitTopology("teststorm", readDefaultConfig, topologyBuilder.createTopology());
		} else {
			// 本地测试
			Config config = new Config();
			config.setNumWorkers(1);
			config.setDebug(false);
			config.setMaxTaskParallelism(5);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("teststorm", config, topologyBuilder.createTopology());
//            Utils.sleep(60000);
//            // 执行完毕，关闭cluster
//            cluster.shutdown();
		}
	}
}
