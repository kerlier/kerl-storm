package com.fashion.storm.kafka.translator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.tuple.Values;

import java.util.List;

public class MyRecordTranslator implements Func<ConsumerRecord<String, String>, List<Object>> {

    private static final long serialVersionUID = 1L;

    @Override
    public List<Object> apply(ConsumerRecord<String, String> record) {
        return new Values(record.value());
    }
}