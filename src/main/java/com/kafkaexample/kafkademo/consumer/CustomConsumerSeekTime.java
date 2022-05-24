package com.kafkaexample.kafkademo.consumer;

import lombok.val;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 消费者按时间消费
 * 把时间转成offset，在按照指定位置处理
 */
public class CustomConsumerSeekTime {
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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        //设置分区分配策略
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,"org.apache.kafka.clients.consumer.RoundRobinAssignor");
        //1.创建一个消费者
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //2.订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);

        //指定位置进行消费
        Set<TopicPartition> assignment = kafkaConsumer.assignment();//得到分区集合

        //保证分区分配方案已经制定完毕
        while (assignment.size() == 0){
            kafkaConsumer.poll(Duration.ofSeconds(1));

            assignment = kafkaConsumer.assignment();
        }

        //---------把时间转成offset，在按照指定位置处理-------
        //希望把时间转换成offset
        HashMap<TopicPartition,Long> topicPartitionerLongHashMap = new HashMap<>();
        //封装对应集合
        for (TopicPartition topicPartition : assignment) {
            topicPartitionerLongHashMap.put(topicPartition,System.currentTimeMillis()-1*24*3600*1000);//获取一天前的offset
        }
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(topicPartitionerLongHashMap);
        //指定消费的offset
        for (TopicPartition topicPartition : assignment) {
            //获取指定时间的offset
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
            kafkaConsumer.seek(topicPartition,offsetAndTimestamp.offset());
        }

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