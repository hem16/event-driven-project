package com.microservice.demo.config;

import lombok.Data;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
@Data
@Configuration
@ConfigurationProperties(prefix =  "twitter-to-kafka-service")
@Slf4j
public class TwitterConfigData {
    private List<String> twitterKeywords;
    private String twitterWelcomeMessage;
}
