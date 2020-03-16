package com.fashion.storm.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * 函数用于接收一个tuple,并且制定接收tuple中哪个field, 它会发射0或者多个tuple,输出的tuple field会追加到原始的tuple后面
 * 没有输出，表示当前的tuple被过滤掉了
 */
public class NumberFunction extends BaseFunction {
    /**
     * Performs the function logic on an individual tuple and emits 0 or more tuples.
     *
     * @param tuple     The incoming tuple
     * @param collector A collector instance that can be used to emit tuples
     */
    public void execute(TridentTuple tuple, TridentCollector collector) {
        // get(0) 表示接收第一个field
        for (int i = 0; i < tuple.getInteger(0); i++) {
            System.out.println("emit " + i);
            collector.emit(new Values(i));
        }
    }
}
