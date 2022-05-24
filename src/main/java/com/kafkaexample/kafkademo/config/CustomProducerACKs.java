package com.kafkaexample.kafkademo.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 数据可靠性设置
 *
 * acks:0就是我的KafkaProducer在客户端，只要把消息发送出去，不管那条数据有没有在哪怕Partition Leader上落到磁盘，
 * 我就不管他了，直接就认为这个消息发送成功了。
 * 如果你采用这种设置的话，那么你必须注意的一点是，可能你发送出去的消息还在半路。
 * 结果呢，Partition Leader所在Broker就直接挂了，然后结果你的客户端还认为消息发送成功了，
 * 此时就会导致这条消息就丢失了。所以这种设置一般用于传输那些不是很重要的数据，
 * 例如ELK中我们使用kafka收集日志的时候，通常都是将acks设为0。
 * acks:1就是说只要Partition Leader接收到消息而且写入本地磁盘了，就认为成功了，
 * 不管他其他的Follower有没有同步过去这条消息了。
 * 这种设置其实是kafka默认的设置，大家请注意，划重点！这是默认的设置。也就是说，默认情况下，
 * 你要是不管acks这个参数，只要Partition Leader写成功就算成功。但是这里有一个问题，
 * 万一Partition Leader刚刚接收到消息，Follower还没来得及同步过去，结果Leader所在的broker宕机了，
 * 此时也会导致这条消息丢失，因为人家客户端已经认为发送成功了。
 * acks:all或-1意思就是说，Partition Leader接收到消息之后，还必须要求ISR列表里跟Leader保持同步的那些Follower
 * 都要把消息同步过去，才能认为这条消息是写入成功了。如果说Partition Leader刚接收到了消息，
 * 但是结果Follower没有收到消息，此时Leader宕机了，那么客户端会感知到这个消息没发送成功，
 * 他会重试再次发送消息过去。此时可能Partition 2的Follower变成Leader了，
 * 此时ISR列表里只有最新的这个Follower转变成的Leader了，那么只要这个新的Leader接收消息就算成功了。
 *
 * 设置acks=all 就可以代表数据一定不会丢失了吗？
 * 当然不是，如果你的Partition只有一个副本，也就是一个Leader，任何Follower都没有，你认为acks=all有用吗？当然没用了，
 * 因为ISR里就一个Leader，他接收完消息后宕机，也会导致数据丢失。所以说，这个acks=all，
 * 必须跟ISR列表里至少有2个以上的副本配合使用，起码是有一个Leader和一个Follower才可以。这样才能保证说写一条数据过去，
 * 一定是2个以上的副本都收到了才算是成功，此时任何一个副本宕机，不会导致数据丢失。
 *
 * producer发送数据重试次数
 * 可能产生的问题：数据重复！
 *
 * 至少一次：ACK级别设置为-1+分区副本大于等于2+ISR里应答的最小副本数量大于等于2
 * 最多一次：CK级别设置为0
 * 精确一次：对于一些重要数据，比如和钱相关的数据，要求数据既不能重复也不丢失，
 * kafka0.11版本以后引入，引入了一项重大特性：幂等性是事务 保证精确一次
 *
 * 开启事务之前必须开启幂等性。
 */
public class CustomProducerACKs {
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
        //acks
        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        //producer发送数据重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
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
