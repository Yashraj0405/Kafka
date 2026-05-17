package com.kp.controller;

import com.kp.dto.Customer;
import com.kp.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    @PostMapping("/publish")
    public ResponseEntity<?> sendEvent(@RequestBody Customer customer){
        try{
            kafkaMessagePublisher.sendEventsToTopic(customer);
            return ResponseEntity.ok("Event published successfully");
        }catch (Exception e){
            System.err.println("Failed to send event: " + e.getMessage());
            return ResponseEntity.status(500).body("Failed to publish event: " + e.getMessage());
        }
     }
}
