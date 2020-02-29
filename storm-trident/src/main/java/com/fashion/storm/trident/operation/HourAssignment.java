package com.fashion.storm.trident.operation;

import com.fashion.storm.trident.pojo.DiagnosisEvent;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * hourAssignment会将tuple中的时间戳转换成时间
 */
public class HourAssignment extends BaseFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(HourAssignment.class);


    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        DiagnosisEvent diagnosisEvent =  (DiagnosisEvent)tridentTuple.get(0);

        String city = tridentTuple.getString(1);

        Long time = diagnosisEvent.getTime();

        //小时
        Long hourSinceEpoch =  time/1000/60/60;

        LOGGER.debug("key : {} , time hour info: {}", city, hourSinceEpoch );

        String key =  city+":" + diagnosisEvent.getDiagnosisCode()+ ":"+ hourSinceEpoch;

        List<Object> values = new ArrayList<Object>();
        values.add(hourSinceEpoch);
        values.add(key);

        tridentCollector.emit(values);

    }
}
