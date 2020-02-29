package com.fashion.storm.trident.state;

import com.fashion.storm.trident.operation.DispatchAlert;
import org.apache.storm.trident.state.map.IBackingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OutbreakTrendBackingMap implements IBackingMap<Long> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutbreakTrendBackingMap.class);

    Map<String,Long> storage=   new ConcurrentHashMap<String,Long>();

    public List<Long> multiGet(List<List<Object>> list) {

        List<Long> values = new ArrayList<Long>();

        for (List<Object> key: list) {

            Long value = storage.get(key.get(0));

            if(value == null){
                values.add(0l);
            }else{
                values.add(value);
            }

        }

        return values;
    }

    public void multiPut(List<List<Object>> list, List<Long> list1) {
        for (int i = 0; i < list.size(); i++) {
            storage.put((String)list.get(i).get(0),list1.get(i));
        }
    }
}
