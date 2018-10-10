package com.victor.kafka.stream;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

public class Application {

    public static void main(String[] args) {
        // 1 配置信息
        Properties props = new Properties();
        //给这个应用程序起一个名字
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logfilter");
        //连接那一台机器
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        // 创建拓扑
        TopologyBuilder builder = new TopologyBuilder();


        builder.addSource("SOURCE", "first")
                .addProcessor("PROCESSOR", new ProcessorSupplier<byte[], byte[]>() {

                    @Override
                    public Processor<byte[], byte[]> get() {

                        return new LogProcessor();
                    }
                }, "SOURCE")
                .addSink("SINK", "second", "PROCESSOR");

        // 创建一个kafka stream 对象
        KafkaStreams streams = new KafkaStreams(builder, props);
        // 发送数据

        streams.start();
    }
}
