package com.microservices.demo.twitter.to.kafka.service.publisher;

import com.twitter.clientlib.model.Tweet;
import lombok.Data;

@Data
public class TweetEvent {
    private Tweet tweet;

    public TweetEvent(Tweet tweet) {
        this.tweet = tweet;
    }
}
