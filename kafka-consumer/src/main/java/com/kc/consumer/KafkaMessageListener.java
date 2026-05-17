package com.kc.consumer;

import com.kc.dto.Customer;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class KafkaMessageListener {

    Logger  logger = org.slf4j.LoggerFactory.getLogger(KafkaMessageListener.class);

    @RetryableTopic(attempts = "5")
    @KafkaListener(topics = "kafka-pro-T-4", groupId = "kafka-consumer")
    public void consumeEvent(Customer customer , @Header(KafkaHeaders.RECEIVED_TOPIC) String topic , @Header(KafkaHeaders.OFFSET) Long offset){
        try {
            List<String> restrictedIPs = Stream.of("32.241.244.236","15.55.49.164").collect(Collectors.toList());
            if(restrictedIPs.contains(customer.getIpAddress())){
                throw new RuntimeException("Message from restricted IP address: " + customer.getIpAddress());
            }
            logger.info(" Consumer - Received message: {}", customer.toString());
        }catch (Exception e){
            logger.error("Failed to process message: " + e.getMessage());
                throw e; // Rethrow the exception to trigger retry and DLT handling
        }
    }

    @DltHandler
    public void listenDLT(Customer customer,@Header(KafkaHeaders.RECEIVED_TOPIC) String topic , @Header(KafkaHeaders.OFFSET) Long offset){
        logger.info("DLT Consumer - Received message: {}", customer.toString());
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
