package com.microservices.demo.twitter.to.kafka.service.listener;

import com.microservices.demo.twitter.to.kafka.service.publisher.StreamedTweetEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class TwitterKafkaStatusListener  {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    @EventListener
    public void streamedTweetListener(StreamedTweetEvent event) {
        LOG.info("Twitter status with text {}", event.getTweet());
    }
}
