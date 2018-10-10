package com.victor.kafka.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]>{

    private ProcessorContext context;

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        // 获取一行
        String line = new String(value);

        if (line.contains(">>>")) {

            line = line.split(">>>")[1].trim();

            context.forward("logProcessor".getBytes(), line.getBytes());

        }else {
            context.forward("logProcessor".getBytes(), value);
        }
    }

    @Override
    public void punctuate(long arg0) {
        // TODO Auto-generated method stub

    }

}

