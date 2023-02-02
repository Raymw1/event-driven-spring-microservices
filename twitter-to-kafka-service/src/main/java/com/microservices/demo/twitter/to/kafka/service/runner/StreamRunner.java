package com.microservices.demo.twitter.to.kafka.service.runner;

import com.twitter.clientlib.ApiException;

public interface StreamRunner {
    void start() throws ApiException;
}
