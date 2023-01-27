package com.microservices.demo.twitter.to.kafka.service.runner;

import com.twitter.clientlib.ApiException;
//import twitter4j.TwitterException;

public interface StreamRunner {
    void start() throws ApiException;
}
