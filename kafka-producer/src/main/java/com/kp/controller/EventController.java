package com.kp.controller;

import com.kp.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/k-producer")
public class EventController {

    @Autowired
    private KafkaMessagePublisher kafkaMessagePublisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> publishMessage(@PathVariable String message){
        try{
            for(int i=0; i<10000; i++){
                kafkaMessagePublisher.sendMessageToTopic(message + " - " + i);
            }
            return ResponseEntity.ok("Message published successfully");
        }catch (Exception e){
            return ResponseEntity.status(500).body("Failed to publish message: " + e.getMessage());
        }
    }
}
