package com.kp.service;

import com.kp.dto.Customer;
import lombok.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<@NonNull String, @NonNull Object> kafkaTemplate;

    public void sendMessageToTopic(String message){
        CompletableFuture<SendResult<@NonNull String, @NonNull Object>> send = kafkaTemplate.send("kafka-pro-T", message);
        send.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Failed to send message: " + ex.getMessage());
            } else {
                System.out.println("Message sent successfully: " + message + "with offset : " + result.getRecordMetadata().offset());
            }
        });
    }

    public void sendEventsToTopic(Customer customer){

        try {
            CompletableFuture<SendResult<@NonNull String, @NonNull Object>> send = kafkaTemplate.send("kafka-pro-T-3", customer);
            send.whenComplete((result, ex) -> {
                if (ex != null) {
                    System.err.println("Failed to send message: " + ex.getMessage());
                } else {
                    System.out.println("Message sent successfully: " + customer.toString() + "with offset : " + result.getRecordMetadata().offset());
                }
            });
        }catch (Exception e){
            System.err.println("Failed to send event: " + e.getMessage());
        }

    }
}
