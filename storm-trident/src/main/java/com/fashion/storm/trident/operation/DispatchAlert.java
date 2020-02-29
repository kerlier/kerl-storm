package com.fashion.storm.trident.operation;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dispatch 发出
 * alert 报警
 */
public class DispatchAlert extends BaseFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(DispatchAlert.class);

    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        String alertInfo = tridentTuple.getString(0);

        LOGGER.error("Alert Received: [" + alertInfo+"] ");

        System.exit(0);
    }
}
