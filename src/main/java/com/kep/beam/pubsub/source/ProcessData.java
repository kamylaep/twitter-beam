package com.kep.beam.pubsub.source;

import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.collections4.CollectionUtils;

import com.google.gson.Gson;

public class ProcessData extends PTransform<KeyedPCollectionTuple<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(KeyedPCollectionTuple<String> input) {
        return input
            .apply(CoGroupByKey.create())
            .apply("FilterSource", ParDo.of(new FilterTweetsFn()))
            .apply("RemoveEmptyTweets", Filter.by(tweetData -> CollectionUtils.isNotEmpty(tweetData.getTweets())))
            .apply("MapTweetDataToJson", MapElements.into(TypeDescriptors.strings()).via(tweetData -> new Gson().toJson(tweetData)));
    }
}