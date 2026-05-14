package com.kc.consumer;

import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    Logger  logger = org.slf4j.LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(topics = "kafka-pro-T", groupId = "kafka-consumer-group")
    public void consume1(String message){
        logger.info(" Consumer 1 - Received message: {}", message);
    }

    @KafkaListener(topics = "kafka-pro-T", groupId = "kafka-consumer-group")
    public void consume2(String message){
        logger.info("Consumer 2 - Received message: {}", message);
    }

    @KafkaListener(topics = "kafka-pro-T", groupId = "kafka-consumer-group")
    public void consume3(String message){
        logger.info("Consumer 3 - Received message: {}", message);
    }

    @KafkaListener(topics = "kafka-pro-T", groupId = "kafka-consumer-group")
    public void consume4(String message){
        logger.info("Consumer 4 - Received message: {}", message);
    }

}
