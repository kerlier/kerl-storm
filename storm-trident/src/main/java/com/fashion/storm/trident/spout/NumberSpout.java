package com.fashion.storm.trident.spout;

import com.fashion.storm.trident.coordinator.DefaultCoordinator;
import com.fashion.storm.trident.emitter.DiagnosisEventEmitter;
import com.fashion.storm.trident.emitter.NumberEmitter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class NumberSpout implements ITridentSpout<Long> {

    private BatchCoordinator coordinator = new DefaultCoordinator();

    private Emitter emitter = new NumberEmitter();
    /**
     *
     * @param txStateId stream id
     * @param conf      Storm config map
     * @param context   topology context
     * @return spout coordinator instance
     */
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return coordinator;
    }

    /**

     * @param txStateId stream id
     * @param conf      Storm config map
     * @param context   topology context
     * @return spout emitter
     */
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return emitter;
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public Fields getOutputFields() {
        return new Fields("age1","age2","age3");
    }
}
