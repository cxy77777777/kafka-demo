package com.kafkaexample.kafkademo.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * 消费一个主题
 */
public class CustomConsumer {
    public static Boolean flag = false;
    public static void main(String[] args) {
        //0配置
        Properties properties = new Properties();
        //链接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"101.201.70.231:9092");
        //反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //配置消费者组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test2");
        //设置分区分配策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.RoundRobinAssignor");
        //1.创建一个消费者
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //2.订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);
        //3.消费数据

        while (true){
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String,String> consumerRecord: consumerRecords) {
                System.out.println("**-----------");
                System.out.println(consumerRecord);
            }

            if (flag){
                break;
            }
        }
    }
}