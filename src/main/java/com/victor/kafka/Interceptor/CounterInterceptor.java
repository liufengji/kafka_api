package com.victor.kafka.Interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CounterInterceptor implements ProducerInterceptor<String, String>{

    private int successCounter = 0;
    private int errorCounter = 0;

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 统计成功是失败次数
        if (exception == null) {
            successCounter++;
        }else {
            errorCounter++;
        }

    }

    @Override
    public void close() {

        System.out.println("success:"+successCounter);
        System.out.println("error:"+errorCounter);

    }

}
