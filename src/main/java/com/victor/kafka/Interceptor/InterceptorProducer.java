package com.victor.kafka.Interceptor;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class InterceptorProducer {

    public static void main(String[] args) {
        // 1 配置信息
        Properties props = new Properties();
        // 服务器名称及端口号
        props.put("bootstrap.servers", "hadoop102:9092");
        // 副本
        props.put("acks", "all");
        // 请求失败尝试次数
        props.put("retries", 0);
        // 批量数据处理大小设置
        props.put("batch.size", 16384);
        // 延时时间
        props.put("linger.ms", 1);
        // 缓存大小
        props.put("buffer.memory", 33554432);

        // 序列化key和value
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu.kafka.interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.kafka.interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        // 2创建生产者对象
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 3 发送消息
        for (int i = 0; i < 100; i++) {

            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)));
        }

        // 4 关闭资源
        producer.close();
    }
}