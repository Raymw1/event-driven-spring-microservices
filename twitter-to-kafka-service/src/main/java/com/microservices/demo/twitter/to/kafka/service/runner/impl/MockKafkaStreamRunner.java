package com.microservices.demo.twitter.to.kafka.service.runner.impl;


import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.service.publisher.TweetEvent;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.model.Tweet;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final ApplicationEventPublisher eventPublisher;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[] {
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetur",
            "adipiscing",
            "elit",
            "Praesent",
            "convallis",
            "arcu",
            "volutpat",
            "egestas",
            "finibus",
            "dolor",
            "sapien",
            "accumsan",
            "augue",
            "at",
            "efficitur"
    };

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
                                 ApplicationEventPublisher eventPublisher) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public void start() throws ApiException {
        String[] keywords = this.twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = this.twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength = this.twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMs = this.twitterToKafkaServiceConfigData.getMockSleepMs();
        LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                Tweet tweet = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                this.eventPublisher.publishEvent(new TweetEvent(tweet));
                sleep(sleepTimeMs);
            }
        });
    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping for waiting new status to create!");
        }
    }

    private Tweet getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        Tweet tweet = new Tweet();
        tweet.setCreatedAt(OffsetDateTime.now());
        tweet.setId(String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)));
        tweet.setText(getRandomTweetContent(keywords,minTweetLength,maxTweetLength));
        tweet.setAuthorId(String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)));
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength-minTweetLength+1)+minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    @NotNull
    private static String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
