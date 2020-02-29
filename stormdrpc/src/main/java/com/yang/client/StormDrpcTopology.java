package com.yang.client;

import com.yang.bolt.PrintBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class StormDrpcTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintBolt.class);

    public static void main(String[] args)  {
        LinearDRPCTopologyBuilder topologyBuilder = new LinearDRPCTopologyBuilder("storm-drpc-test");
        topologyBuilder.addBolt(new PrintBolt(), 1).allGrouping();
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2); //应该设置跟supervisor数量一致

        //并行度如何f设置
//        if (args == null || args.length == 0) {
//            LocalDRPC localDRPC = new LocalDRPC();
//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("strom-drpc-topology",conf, topologyBuilder.createLocalTopology(localDRPC));
//            System.out.println("执行本地drpc");
//            String result = localDRPC.execute("storm-drpc-test", "yangyuguang");
//            System.out.println("执行结果:"+result);
//            cluster.shutdown();
//            localDRPC.shutdown();
//        }else{
        try {
            LOGGER.info("drpc function name: " + args[0]);
            StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createRemoteTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
