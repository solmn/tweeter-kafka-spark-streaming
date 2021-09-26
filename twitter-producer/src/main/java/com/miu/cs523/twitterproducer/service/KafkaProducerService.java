package com.miu.cs523.twitterproducer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    public void send(String topic, String message) {
    	//logger.info("Sending payload='{}' to topic='{}'", message, topic);
    	kafkaTemplate.send(topic, message);
    }
}
