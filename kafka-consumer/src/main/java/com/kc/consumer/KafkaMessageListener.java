package com.kc.consumer;

import com.kc.dto.Customer;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    Logger  logger = org.slf4j.LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(topics = "kafka-pro-T-4", groupId = "kafka-consumer")
    public void consumeEvent(Customer customer){
        logger.info(" Consumer - Received message: {}", customer.toString());
    }

    //Listing to specific partition of the topic
//    @KafkaListener( groupId = "kafka-consumer", topicPartitions = {@TopicPartition(topic = "kafka-pro-T-4", partitions = {"3"})})
//    public void consume2(String message){
//        logger.info("Consumer 2 - Received message: {}", message);
//    }
//
//    @KafkaListener(topics = "kafka-pro-T", groupId = "kafka-consumer-group")
//    public void consume3(String message){
//        logger.info("Consumer 3 - Received message: {}", message);
//    }
//
//    @KafkaListener(topics = "kafka-pro-T", groupId = "kafka-consumer-group")
//    public void consume4(String message){
//        logger.info("Consumer 4 - Received message: {}", message);
//    }

}
