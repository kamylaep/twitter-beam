package com.kep.beam.pubsub;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import lombok.Value;

public class PubSubTwitterSourceBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubTwitterSourceBean.class);

    private static final TupleTag<Map<String, String>> USER_TAG = new TupleTag<>();
    private static final TupleTag<Map<String, String>> TWEET_TAG = new TupleTag<>();

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PubSubBeamOptions.class);
        PubSubBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubBeamOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        LOGGER.debug("Registering app options={}", options);

        String project = "projects/" + options.getProject();
        String userSubscription = project + "/subscriptions/" + options.getUserInput();
        String tweetSubscription = project + "/subscriptions/" + options.getTweetInput();
        String topicOut = project + "/topics/" + options.getOutput();

        LOGGER.debug("Building pipeline");

        PCollection<KV<String, Map<String, String>>> users = pipeline
            .apply("ReadUsersTopic", PubsubIO.readStrings().fromSubscription(userSubscription))
            .apply("UserWindow", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds()))))
            .apply("UserJsonToVK", ParDo.of(new JsonToKVFn()));

        PCollection<KV<String, Map<String, String>>> tweets = pipeline
            .apply("ReadTweetsTopic", PubsubIO.readStrings().fromSubscription(tweetSubscription))
            .apply("TweetWindow", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds()))))
            .apply("TweetJsonToKV", ParDo.of(new JsonToKVFn()));

        KeyedPCollectionTuple.of(USER_TAG, users).and(TWEET_TAG, tweets)
            .apply("ProcessData", new ProcessData())
            .apply("SaveIt", PubsubIO.writeStrings().to(topicOut));

        LOGGER.debug("Starting pipeline");
        pipeline.run().waitUntilFinish();
    }

    public static class JsonToKVFn extends DoFn<String, KV<String, Map<String, String>>> {

        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<KV<String, Map<String, String>>> outputReceiver) {
            Map<String, String> map = new Gson().fromJson(json, Map.class);
            outputReceiver.output(KV.of(map.get("user.id"), map));
        }
    }

    public static class FilterTweetsFnKVFn extends DoFn<KV<String, CoGbkResult>, TweetData> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, CoGbkResult> e = c.element();
            Iterable<Map<String, String>> user = e.getValue().getAll(USER_TAG);
            Iterable<Map<String, String>> tweets = e.getValue().getAll(TWEET_TAG);

            PubSubBeamOptions pubsubOptions = c.getPipelineOptions().as(PubSubBeamOptions.class);

            List<String> tweetsFromSource = new ArrayList<>();
            tweets.forEach(tweet -> {
                if (StringUtils.contains(tweet.get("source").toLowerCase(), pubsubOptions.getTweetSource().toLowerCase())) {
                    tweetsFromSource.add(tweet.get("text"));
                }
            });
            Iterator<Map<String, String>> iterator = user.iterator();
            if (iterator.hasNext()) {
                c.output(TweetData.of(iterator.next().get("user.screen_name"), tweetsFromSource));
            }
        }
    }

    public static class ProcessData extends PTransform<KeyedPCollectionTuple<String>, PCollection<String>> {

        @Override
        public PCollection<String> expand(KeyedPCollectionTuple<String> input) {
            return input
                .apply(CoGroupByKey.create())
                .apply("FilterSource", ParDo.of(new FilterTweetsFnKVFn()))
                .apply("RemoveEmptyTweets", Filter.by(tweetData -> CollectionUtils.isNotEmpty(tweetData.getTweets())))
                .apply("MapTweetDataToJson", MapElements.into(TypeDescriptors.strings()).via(tweetData -> new Gson().toJson(tweetData)));
        }
    }

    @Value(staticConstructor = "of")
    public static class TweetData implements Serializable {

        private String username;
        private List<String> tweets;
    }

}