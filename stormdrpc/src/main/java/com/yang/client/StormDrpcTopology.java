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

@SuppressWarnings("deprecation")
public class StormDrpcTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        LinearDRPCTopologyBuilder topologyBuilder = new LinearDRPCTopologyBuilder("storm-drpc-test");
        topologyBuilder.addBolt(new PrintBolt(), 1).allGrouping();
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2); //应该设置跟supervisor数量一致

        //并行度如何设置
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
        StormSubmitter.submitTopology("storm-drpc", conf, topologyBuilder.createRemoteTopology());
//        }
    }
}
