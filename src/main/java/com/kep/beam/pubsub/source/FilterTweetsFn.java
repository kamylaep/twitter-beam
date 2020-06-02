package com.kep.beam.pubsub.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;

public class FilterTweetsFn extends DoFn<KV<String, CoGbkResult>, TweetData> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> e = c.element();
        Iterable<Map<String, String>> user = e.getValue().getAll(TwitterSourcePipeline.USER_TAG);
        Iterable<Map<String, String>> tweets = e.getValue().getAll(TwitterSourcePipeline.TWEET_TAG);

        SourceOptions pubsubOptions = c.getPipelineOptions().as(SourceOptions.class);

        List<String> tweetsFromSource = new ArrayList<>();
        tweets.forEach(tweet -> {
            if (StringUtils.contains(tweet.get("source").toLowerCase(), pubsubOptions.getTweetSource().toLowerCase())) {
                tweetsFromSource.add(tweet.get("text"));
            }
        });

        Collections.sort(tweetsFromSource);
        Iterator<Map<String, String>> iterator = user.iterator();
        if (iterator.hasNext()) {
            c.output(TweetData.of(iterator.next().get("user.screen_name"), tweetsFromSource));
        }
    }
}