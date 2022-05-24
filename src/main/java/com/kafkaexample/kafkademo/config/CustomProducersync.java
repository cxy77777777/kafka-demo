package com.kafkaexample.kafkademo.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * kafka生产者同步发送数据
 */
public class CustomProducersync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //0.配置
        Properties properties = new Properties();
        //连接kafka集群bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"101.201.70.231:9092");
        //指定对应的key和value序列化类型org.apache.kafka.common.serialization.StringSerializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //2.发送数据,加上.get()既是同步发送，去掉既是异步发送
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","Holle1" + i)).get();
        }
        //3.关闭资源
        kafkaProducer.close();
    }
}
