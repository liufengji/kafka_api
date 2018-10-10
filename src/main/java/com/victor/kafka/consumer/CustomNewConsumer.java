package com.victor.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CustomNewConsumer {

    // 消费者
    @SuppressWarnings("resource")
    public static void main(String[] args) {

        // 1 设置配置信息
        Properties props = new Properties();
        // 服务器
        props.put("bootstrap.servers", "hadoop102:9092");
        // 消费者组id
        props.put("group.id", "test");
        // 自动处理
        props.put("enable.auto.commit", "true");
        // 时间设置
        props.put("auto.commit.interval.ms", "1000");
        // 序列化的key、value
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 3 订阅消费的topic
        consumer.subscribe(Arrays.asList("first", "second", "three", "four"));

        // 4 具体的消费过程
        while (true) {
            // 获取消费数据
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic:" + record.topic() + "key:" + record.key() + " ;value:" + record.value());
            }

        }

        // KafkaConsumer&lt;String, String&gt; consumer = new
        // KafkaConsumer&lt;&gt;(props);
        // consumer.subscribe(Arrays.asList("foo", "bar"));
        // while (true) {
        // ConsumerRecords&lt;String, String&gt; records = consumer.poll(100);
        // for (ConsumerRecord&lt;String, String&gt; record : records)
        // System.out.printf("offset = %d, key = %s, value = %s%n",
        // record.offset(), record.key(), record.value());
        // }
    }
}
