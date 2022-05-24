package com.kafkaexample.kafkademo.config;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * kafka生产者---发送数据并指定分区
 */
public class CustomProducerCallbackPartitions {
    private static final Logger log = LoggerFactory.getLogger(CustomProducerCallbackPartitions.class);
    public static void main(String[] args) {
        //0.配置
        Properties properties = new Properties();
        //连接kafka集群bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"101.201.70.231:9092");
        //指定对应的key和value序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.kafkaexample.kafkademo.config.MyPartitioner");
        //1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //2.发送数据,
        for (int i = 0; i < 2; i++) {
            //指定分区partition参数优先级最高，大于按key的hash值取模分区，,顺序partition参数>自定义分区>按key的hash值取模分区
//            kafkaProducer.send(new ProducerRecord<>("first",2,"f", "Holle" + i), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e == null){
//                        log.info("主题：" + recordMetadata.topic() + ",分区：" + recordMetadata.partition());
//                    }
//                }
//            });
            //使用自定义分区器
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
