package com.fashion.storm.trident.operation;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class OutBreakDetector extends BaseFunction {

    //门槛
    private static final Integer THRESHOLD = 10000;
    
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        String key = tridentTuple.getString(0);

        Long count = tridentTuple.getLong(1);

        //超过阈值的话，就会发送数据
        if(count>THRESHOLD ){
            List<Object> values = new ArrayList<Object>();
            values.add("Outbreak detected for [" +key+ "] ");
            tridentCollector.emit(values);
        }
    }
}
