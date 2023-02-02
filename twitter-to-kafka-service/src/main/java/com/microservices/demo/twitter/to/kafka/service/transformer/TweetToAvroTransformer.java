package com.microservices.demo.twitter.to.kafka.service.transformer;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.twitter.clientlib.model.Tweet;
import org.springframework.stereotype.Component;

@Component
public class TweetToAvroTransformer {

    public TwitterAvroModel getTwitterAvroModelFromTweet(Tweet tweet) {
        return TwitterAvroModel
                .newBuilder()
                .setId(Long.parseLong(tweet.getId()))
                .setUserId(Long.parseLong(tweet.getAuthorId()))
                .setText(tweet.getText())
                .setCreatedAt(tweet.getCreatedAt().toEpochSecond())
                .build();
    }
}
