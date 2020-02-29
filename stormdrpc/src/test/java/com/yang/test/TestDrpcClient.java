package com.yang.test;

import org.apache.storm.Config;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.Map;

public class TestDrpcClient {

    @Test
    public void testDrpcClient() throws TException {
        Map config = Utils.readDefaultConfig();
        DRPCClient drpcClient = new DRPCClient(config, "192.168.5.134", 3772);
        for (int i = 0; i < 60; i++) {
            String result = drpcClient.execute("storm-drpc-test", "aaa" + i);
            System.out.println(result);
        }
    }
    @Test
    public void testDrpcClient1() throws TException {
        Map config = Utils.readDefaultConfig();
        DRPCClient drpcClient = new DRPCClient(config, "192.168.5.134", 3772);
        for (int i = 60; i < 70; i++) {
            String result = drpcClient.execute("storm-drpc-test", "aaa" + i);
            System.out.println(result);
        }
    }
}
