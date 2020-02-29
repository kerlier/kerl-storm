package com.fashion.storm.trident.emitter;

import com.fashion.storm.trident.pojo.DiagnosisEvent;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * trident storm: coordinator, emitter, 还有operation( filter,function)
 * trident storm 与 storm 的区别：
 * storm中使用spout来发射数据
 * trident storm 使用emitter发射数据，(数据是batchCoordinator初始化的一批数据)
 */
public class DiagnosisEventEmitter implements ITridentSpout.Emitter<Long>, Serializable {

    AtomicInteger successTransactions =  new AtomicInteger(0);

    /**
     * emitBatch 是真正发射tuple的方法
     * @param transactionAttempt
     * @param aLong
     * @param tridentCollector
     */
    public void emitBatch(TransactionAttempt transactionAttempt, Long aLong, TridentCollector tridentCollector) {

        //构造数据
        for (int i = 0; i <10000 ; i++) {

            //这里的events是一个Object的list
            List<Object> events = new ArrayList<Object>();

            //lat纬度
            double lat = new Double(-30 + (int)(Math.random()*75));

            //lng经度
            double lng = new Double(-120 + (int)(Math.random()*70));

            long time = System.currentTimeMillis();

            //生成ICD-9-Cm的疾病编码
            String diag = new Integer(320+ (int)(Math.random()*7)).toString();

            DiagnosisEvent diagnosisEvent = new DiagnosisEvent(lat, lng, time, diag);

            events.add(diagnosisEvent);

            tridentCollector.emit(events);
        }
    }

    public void success(TransactionAttempt transactionAttempt) {
        //atomicInteger是一个线程安全的类,
        successTransactions.incrementAndGet();
    }

    public void close() {

    }
}
