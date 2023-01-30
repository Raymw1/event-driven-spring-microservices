package com.microservices.demo.twitter.to.kafka.service.publisher;

import lombok.Data;

@Data
public class StreamedTweetEvent {
    private String tweet;

    public StreamedTweetEvent(String tweet) {
        this.tweet = tweet;
    }
}
