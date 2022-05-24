package com.kafkaexample.kafkademo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * kafka生产者回调异步发送数据
 */
public class CustomProducerCallback {
    private static final Logger log = LoggerFactory.getLogger(CustomProducerCallback.class);
    public static void main(String[] args) {
        //0.配置
        Properties properties = new Properties();
        //连接kafka集群bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"101.201.70.231:9092");
        //指定对应的key和value序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //2.发送数据
        for (int i = 0; i < 2; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "Holle" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        log.info("主题：" + recordMetadata.topic() + ",分区：" + recordMetadata.partition());
                    }
                }
            });
        }
        //3.关闭资源
        kafkaProducer.close();
    }
}
