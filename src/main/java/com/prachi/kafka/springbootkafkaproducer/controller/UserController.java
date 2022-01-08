package com.prachi.kafka.springbootkafkaproducer.controller;

import com.prachi.kafka.springbootkafkaproducer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("kafka")
@Slf4j
public class UserController {

    @Autowired
    @Qualifier("DefaultConfig")
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, User> userKafkaTemplate;

    @Autowired
    @Qualifier("DiffBootStrapServer")
    private KafkaTemplate<String, String> kafkaTemplateQualifier;

    private static final String TOPIC = "topic_string_data";

    @GetMapping(value = "/string/{message}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String publishString(@PathVariable("message") String message ) {
        kafkaTemplate.send(TOPIC, message);
        kafkaTemplateQualifier.send(TOPIC, message);
        return "Hello World published ";
    }

    @PostMapping(value = "/json", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<User> publishJson(@RequestBody User user){
        ListenableFuture<SendResult<String, User>> send = userKafkaTemplate.send(TOPIC, user);
        final ResponseEntity<User>[] responseEntity = new ResponseEntity[]{null};
        send.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {
            @Override
            public void onFailure(Throwable throwable) {
                 responseEntity[0] = new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
            }

            @Override
            public void onSuccess(SendResult<String, User> stringUserSendResult) {
                 responseEntity[0] = new ResponseEntity<>(HttpStatus.OK);
                log.info("Produced successfully \n "+stringUserSendResult.getRecordMetadata());
            }
        });
        return responseEntity[0];
    }

}
