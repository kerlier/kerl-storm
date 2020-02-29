package com.fashion.storm.trident.spout;

import com.fashion.storm.trident.coordinator.DefaultCoordinator;
import com.fashion.storm.trident.emitter.DiagnosisEventEmitter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * trident: 三叉戟
 *
 * 在trident中,spout没有真的发射tuple, 发射的工作分给了batchCoordinator还有emitter这个组件
 *
 */
public class DiagnosisEventSpout implements ITridentSpout<Long> {

    /**
     * spout中使用 coordinator 和emitter来发送数据
     */

    private BatchCoordinator coordinator = new DefaultCoordinator();

    private Emitter emitter = new DiagnosisEventEmitter();

    /**
     * 协调器,需要自己实现
     * @param s
     * @param map
     * @param topologyContext
     * @return
     */
    public BatchCoordinator<Long> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return coordinator;
    }

    /**
     * 编辑器，需要自己实现
     * @param s
     * @param map
     * @param topologyContext
     * @return
     */

    public Emitter<Long> getEmitter(String s, Map map, TopologyContext topologyContext) {
        return emitter;
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public Fields getOutputFields() {
        return new Fields("event");
    }
}
