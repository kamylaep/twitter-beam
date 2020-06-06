package com.kep.beam.pubsub.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;

public class FilterTweetsFn extends DoFn<KV<String, CoGbkResult>, OutputTweetData> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> e = c.element();
        Iterable<InputUserData> user = e.getValue().getAll(TwitterSourcePipeline.USER_TAG);
        Iterable<InputTweetData> tweets = e.getValue().getAll(TwitterSourcePipeline.TWEET_TAG);

        List<String> tweetsFromSource = filterTweetsFromSource(c.getPipelineOptions().as(SourceOptions.class), tweets);
        OutputTweetData outputTweetData = formatOutput(user, tweetsFromSource);
        c.output(outputTweetData);
    }

    private List<String> filterTweetsFromSource(SourceOptions pubsubOptions, Iterable<InputTweetData> tweets) {
        List<String> tweetsFromSource = new ArrayList<>();
        tweets.forEach(tweet -> {
            if (StringUtils.containsIgnoreCase(tweet.getSource(), pubsubOptions.getTweetSource())) {
                tweetsFromSource.add(tweet.getText());
            }
        });
        return tweetsFromSource;
    }

    private OutputTweetData formatOutput(Iterable<InputUserData> user, List<String> tweetsFromSource) {
        Collections.sort(tweetsFromSource);
        return StreamSupport.stream(user.spliterator(), false)
            .findFirst()
            .map(u -> OutputTweetData.of(u.getScreenName(), tweetsFromSource))
            .orElse(null);
    }
}