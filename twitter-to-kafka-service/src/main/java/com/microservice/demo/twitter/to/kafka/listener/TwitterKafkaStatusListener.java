package com.microservice.demo.twitter.to.kafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;
@Component
@Slf4j
public class TwitterKafkaStatusListener extends StatusAdapter {

    @Override
    public void onStatus(Status status) {
      log.info("Twitter status with text {}",status.getText());
    }
}
