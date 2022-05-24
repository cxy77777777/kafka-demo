package com.kafkaexample.kafkademo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Product生产消息
 */
@RestController
public class ProductController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @GetMapping("datas")
    public String data(String s){
        kafkaTemplate.send("first",s);
        return s;
    }
}
