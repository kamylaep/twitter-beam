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

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

public class PubSubTwitterSourceBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubTwitterSourceBean.class);

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

    MapElements<String, KV<String, Map<String, String>>> jsonToKV = MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
        TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.strings())))
        .via(json -> {
          Map<String, String> map = new Gson().fromJson(json, Map.class);
          return KV.of(map.get("user.id"), map);
        });

    PCollection<KV<String, Map<String, String>>> users = pipeline.apply("ReadUsersTopic", PubsubIO.readStrings().fromSubscription(userSubscription))
        .apply("UserWindow", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds()))))
        .apply("UserJsonToVK", jsonToKV);

    PCollection<KV<String, Map<String, String>>> tweets = pipeline.apply("ReadTweetsTopic", PubsubIO.readStrings().fromSubscription(tweetSubscription))
        .apply("TweetWindow", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds()))))
        .apply("TweetJsonToKV", jsonToKV);

    TupleTag<Map<String, String>> userTag = new TupleTag<>();
    TupleTag<Map<String, String>> tweetTag = new TupleTag<>();

    KeyedPCollectionTuple.of(userTag, users).and(tweetTag, tweets)
        .apply(CoGroupByKey.create())
        .apply("FilterSource", ParDo.of(new DoFn<KV<String, CoGbkResult>, TweetData>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            KV<String, CoGbkResult> e = c.element();
            Iterable<Map<String, String>> user = e.getValue().getAll(userTag);
            Iterable<Map<String, String>> tweets = e.getValue().getAll(tweetTag);

            PubSubBeamOptions pubsubOptions = c.getPipelineOptions().as(PubSubBeamOptions.class);

            List<String> tweetsFromSource = new ArrayList<>();
            tweets.forEach(tweet -> {
              if (StringUtils.contains(tweet.get("source").toLowerCase(), pubsubOptions.getTweetSource().toLowerCase())) {
                tweetsFromSource.add(tweet.get("text"));
              }
            });
            Iterator<Map<String, String>> iterator = user.iterator();
            if (iterator.hasNext()) {
              c.output(TweetData.builder().username(iterator.next().get("user.screen_name")).tweets(tweetsFromSource).build());
            }
          }
        }))
        .apply("RemoveEmptyTweets", Filter.by(tweetData -> CollectionUtils.isNotEmpty(tweetData.getTweets())))
        .apply("MapTweetDataToJson", MapElements.into(TypeDescriptors.strings()).via(tweetData -> new Gson().toJson(tweetData)))
        .apply("SaveIt", PubsubIO.writeStrings().to(topicOut));

    LOGGER.debug("Starting pipeline");
    pipeline.run().waitUntilFinish();
  }

  @Builder
  @Getter
  @EqualsAndHashCode
  public static class TweetData implements Serializable {

    private String username;
    private List<String> tweets;
  }

}