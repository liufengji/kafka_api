package com.victor.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CallBackProducer {

    //创建生产者带回调函数（新API）
    public static void main(String[] args) {

        //1、配置信息
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

        //2、创建生产者对象
        Producer<String, String> producer = new KafkaProducer<>(props);

        //3、发送消息
        for (int i = 0; i < 1000; i++) {
           //producer.send(new ProducerRecord<String, String>("second", Integer.toString(i), Integer.toString(i)));

            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)), new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (metadata !=null) {
                        System.out.println("分区："+metadata.partition()+"  偏移量："+metadata.offset());
                    }
                }
            });

        }

        // 4 关闭资源
        producer.close();
    }
}
