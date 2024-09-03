package com.microservice.demo.twitter.to.kafka.runner.Impl;

import com.microservice.demo.config.TwitterConfigData;
import com.microservice.demo.twitter.to.kafka.listener.TwitterKafkaStatusListener;
import com.microservice.demo.twitter.to.kafka.runner.StreamRunner;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;

@Component
@Slf4j
public class KafkaStreamRunnerImpl implements StreamRunner {

    private final TwitterConfigData TwitterConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    public TwitterStream twitterStream;

    public KafkaStreamRunnerImpl(TwitterConfigData twitterConfigData,
                                 TwitterKafkaStatusListener twitterKafkaStatusListener) {
        TwitterConfigData = twitterConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        addFilter();
    }

    @PreDestroy
    public void shutdown(){
         if(twitterStream != null) {
             log.info("Closing twitter stream!");
             twitterStream.shutdown();
         }
    }

    private void addFilter() {
        String [] keywords = TwitterConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        log.info("Started filtering  twitter stream for Keywords {}", Arrays.toString(keywords));
    }
}
