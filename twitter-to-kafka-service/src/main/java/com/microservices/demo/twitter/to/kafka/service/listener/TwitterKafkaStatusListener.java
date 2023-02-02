package com.microservices.demo.twitter.to.kafka.service.listener;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import com.microservices.demo.kafka.producer.config.service.KafkaProducer;
import com.microservices.demo.twitter.to.kafka.service.publisher.TweetEvent;
import com.microservices.demo.twitter.to.kafka.service.transformer.TweetToAvroTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class TwitterKafkaStatusListener  {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    private final KafkaConfigData kafkaConfigData;

    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

    private final TweetToAvroTransformer tweetToAvroTransformer;

    public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData, KafkaProducer<Long, TwitterAvroModel> kafkaProducer, TweetToAvroTransformer tweetToAvroTransformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducer = kafkaProducer;
        this.tweetToAvroTransformer = tweetToAvroTransformer;
    }

    @EventListener
    public void streamedTweetListener(TweetEvent event) {
        LOG.info("Twitter status with text {} sending to kafka topic {}.", event.getTweet().getText(), kafkaConfigData.getTopicName());
        TwitterAvroModel twitterAvroModel = tweetToAvroTransformer.getTwitterAvroModelFromTweet(event.getTweet());
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }
}
