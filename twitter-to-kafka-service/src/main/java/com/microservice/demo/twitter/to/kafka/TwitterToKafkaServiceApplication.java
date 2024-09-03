package com.microservice.demo.twitter.to.kafka;

import com.microservice.demo.config.TwitterConfigData;
import com.microservice.demo.twitter.to.kafka.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservice.demo")
@Slf4j
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private final TwitterConfigData configData;
    private final StreamRunner streamRunner;
    public TwitterToKafkaServiceApplication(TwitterConfigData configData,
                                            StreamRunner streamRunner){
        this.configData = configData;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Application Starts");
        log.info(Arrays.toString(configData.getTwitterKeywords().toArray(new String[] {})));
        log.info(configData.getTwitterWelcomeMessage());
        streamRunner.start();
    }
}
