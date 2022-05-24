package com.kafkaexample.kafkademo.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 提高kafka吞吐量配置
 * 1.缓冲区大小
 * 2.批次大小
 * 3.linger.ms
 * 4.压缩
 */
public class CustomProducerParameters {
    public static void main(String[] args) {
        //0.配置
        Properties properties = new Properties();
        //连接kafka集群bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"101.201.70.231:9092");
        //指定对应的key和value序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);//32m
        //批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);//16k
        //linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);//1ms
        //压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");//32m
        //1.创建kafka生产者对象
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        //2.发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","sss"));
        }
        //3.关闭连接
        kafkaProducer.close();
    }
}
