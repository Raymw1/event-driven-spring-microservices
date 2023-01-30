package com.microservices.demo.twitter.to.kafka.service.model;

import com.twitter.clientlib.model.FilteredStreamingTweetResponse;
import lombok.Data;

@Data
public class StreamedTweet extends FilteredStreamingTweetResponse {
    private String createdAt;
    private String id;
    private String text;
    private String authorId;
}
