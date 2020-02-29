package com.fashion.storm.trident.operation;

import com.fashion.storm.trident.pojo.DiagnosisEvent;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * filter 属于operation 的一种
 * 只是过滤tuple,对tuple中的值没有做操作
 *
 */
public class DiseaseFilter extends BaseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiseaseFilter.class);

    /**
     * 只过滤疾病的code<=322的疾病
     * @param tridentTuple
     * @return
     */
    public boolean isKeep(TridentTuple tridentTuple) {

        DiagnosisEvent diagnosisEvent =  (DiagnosisEvent)tridentTuple.get(0);

        int diagnosisCode = Integer.parseInt(diagnosisEvent.getDiagnosisCode());

        if(diagnosisCode <= 322){
            LOGGER.debug("Emitting Disease : " + diagnosisCode );
            //return true的tuple,会发送到下游进行操作,return false,不会发送到下游
            return true;
        }else{
            LOGGER.debug("Filtering Disease: " + diagnosisCode);
            return false;
        }
    }
}
