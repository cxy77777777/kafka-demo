package com.kafkaexample.kafkademo.controller;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Consumer消费消息
 */
@Configuration
public class KafkaConsumer {
    @KafkaListener(topics = "first")
    public String data(String s){
        System.out.println("收到的消息：" + s);
        return s;
    }
}
