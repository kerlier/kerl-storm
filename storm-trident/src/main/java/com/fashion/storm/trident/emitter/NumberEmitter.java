package com.fashion.storm.trident.emitter;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Values;
import sun.awt.SunHints;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class NumberEmitter implements ITridentSpout.Emitter<Long>, Serializable {
    AtomicInteger successTransactions =  new AtomicInteger(0);

    /**
     *
     * emitBatch是提交batch的真正方法
     *
     * @param tx              transaction id
     * @param coordinatorMeta metadata from the coordinator defining this transaction
     * @param collector       output tuple collector
     */
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
        collector.emit(new Values(1,1,34));
    }

    /**
     * This attempt committed successfully, so all state for this commit and before can be safely cleaned up.
     *
     * @param tx attempt object containing transaction id and attempt number
     */
    public void success(TransactionAttempt tx) {
        //atomicInteger是一个线程安全的类,
        successTransactions.incrementAndGet();
    }

    /**
     * Release any resources held by this emitter.
     */
    public void close() {

    }
}
