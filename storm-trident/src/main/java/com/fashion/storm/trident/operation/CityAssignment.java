package com.fashion.storm.trident.operation;

import com.fashion.storm.trident.pojo.DiagnosisEvent;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * function作用：在tuple中加上某些参数,原来的tuple的值不会被改变
 * 然后将tuple再发送出去
 *
 * 根据经纬度算出属于是哪个城市，然后再发送出去
 */
public class CityAssignment extends BaseFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(CityAssignment.class);

    private static  HashMap<String,double[]> CITIES = new HashMap<String,double[]>();

    {
        double[] phl = {39.87, -75.24};
        CITIES.put("PHL",phl);

        double[] nyc = {40.71, -74.00};
        CITIES.put("NYC",nyc);

        double[] sf = {-31.42, -62.08};
        CITIES.put("SF",sf);

        double[] la = {-34.05, -118.24};
        CITIES.put("LA",la);
    }


    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        DiagnosisEvent diagnosisEvent =  (DiagnosisEvent)tridentTuple.get(0);

        double leastDistance = Double.MAX_VALUE;

        String closestCity = "NONE";

        for (Map.Entry<String, double[]> entry: CITIES.entrySet()) {

            double R=6371;
            double x = (entry.getValue()[0] - diagnosisEvent.getLng())* Math.cos((entry.getValue()[0] + diagnosisEvent.getLng()) / 2);
            double y = entry.getValue()[1] - diagnosisEvent.getLng();
            double d = Math.sqrt(x*x+y*y) * R;

            if( d < leastDistance){
                leastDistance = d;
                closestCity = entry.getKey();
            }
        }

        //找到离得最近的城市距离
        List<Object> values = new ArrayList<Object>();
        values.add(closestCity);

        //如果debug被禁掉的话，性能要比拼接参数的那种方式好上30倍
        LOGGER.debug("Closet city for lat: {} and lng: {} is {} ." ,closestCity ,diagnosisEvent.getLat(),diagnosisEvent.getLng());


        //这里的values是一个list,因为可能在stream中的操作中会有 new Fields("field1","field2")这种操作
        tridentCollector.emit(values); //这个collector会将新字段添加到tuple中，不会删除或者变更已有的字段
    }
}
