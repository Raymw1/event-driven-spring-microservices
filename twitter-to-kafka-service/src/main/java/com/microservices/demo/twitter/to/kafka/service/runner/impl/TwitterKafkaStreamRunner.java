package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.google.common.reflect.TypeToken;
import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.JSON;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.*;

@Component
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterApi twitterApi;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData configData,
                                    TwitterKafkaStatusListener statusListener) {
        this.twitterToKafkaServiceConfigData = configData;
        this.twitterKafkaStatusListener = statusListener;
    }

    @Override
    public void start() throws ApiException {
        try {
            twitterApi = new TwitterApi(new TwitterCredentialsBearer(System.getenv("twitter_bearer_token")));

            setupRules();

            Set<String> userFields = new HashSet<>(Arrays.asList(User.SERIALIZED_NAME_USERNAME));

            InputStream result = twitterApi.tweets().searchStream().userFields(userFields).execute();
            try {
                JSON json = new JSON();

                Type localVarReturnType = new TypeToken<FilteredStreamingTweetResponse>(){}.getType();
                BufferedReader reader = new BufferedReader(new InputStreamReader(result));
                String line = reader.readLine();

                while (line != null) {
                    if(line.isEmpty()) {
                        line = reader.readLine();
                        continue;
                    }
                    LOG.info(line);
                    Object jsonObject = json.getGson().fromJson(line, localVarReturnType);
                    System.out.println(jsonObject != null ? jsonObject.toString() : "Null object");
                    line = reader.readLine();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }
        } catch (ApiException e) {
            LOG.info("Exception when calling TweetsApi#searchStream");
            LOG.info("Status code: " + e.getCode());
            LOG.info("Reason: " + e.getResponseBody());
            LOG.info("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }

    private void setupRules() throws ApiException {
        List<Rule> rules = getRules();
        if (rules != null) {
            deleteRules(rules);
        }
        addRules();
    }

    private void addRules() throws ApiException {
        AddOrDeleteRulesRequest addOrDeleteRulesRequest = new AddOrDeleteRulesRequest();
        AddRulesRequest addRuleRequest = new AddRulesRequest();

        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);

        for (String keyword : keywords) {
            RuleNoId newRule = new RuleNoId();
            newRule.value(keyword);
            newRule.tag(keyword);
            addRuleRequest.addAddItem(newRule);
        }

        addOrDeleteRulesRequest.setActualInstance(addRuleRequest);
        twitterApi.tweets().addOrDeleteRules(addOrDeleteRulesRequest).dryRun(false).execute();
    }

    private void deleteRules(List<Rule> rules) throws ApiException {
        AddOrDeleteRulesRequest addOrDeleteRulesRequest = new AddOrDeleteRulesRequest();

        List<String> ids = getRulesIds(rules);

        DeleteRulesRequest deleteRulesRequest = new DeleteRulesRequest();
        DeleteRulesRequestDelete deleteRules = new DeleteRulesRequestDelete();

        deleteRules.ids(ids);
        deleteRulesRequest.delete(deleteRules);

        addOrDeleteRulesRequest.setActualInstance(deleteRulesRequest);
        AddOrDeleteRulesResponse result = twitterApi.tweets().addOrDeleteRules(addOrDeleteRulesRequest).dryRun(false).execute();
    }

    private List<String> getRulesIds(List<Rule> rules) {
        List<String> rulesIds = new ArrayList<>();

        for (Rule rule : rules) {
            rulesIds.add(rule.getId());
        }

        return rulesIds;
    }

    private List<Rule> getRules() throws ApiException {
        List<Rule> rules = twitterApi.tweets().getRules().execute().getData();
        return  rules;
    }

}
