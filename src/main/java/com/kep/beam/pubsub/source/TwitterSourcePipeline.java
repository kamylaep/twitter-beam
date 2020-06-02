package com.kep.beam.pubsub.source;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;

public class TwitterSourcePipeline {

    public static final TupleTag<Map<String, String>> USER_TAG = new TupleTag<>();
    public static final TupleTag<Map<String, String>> TWEET_TAG = new TupleTag<>();

    public static void main(String[] args) {
        PipelineOptionsFactory.register(SourceOptions.class);
        SourceOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SourceOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        String userSubscription = ProjectSubscriptionName.format(options.getProject(), options.getUserInput());
        String tweetSubscription = ProjectSubscriptionName.format(options.getProject(), options.getTweetInput());;
        String topicOut = ProjectTopicName.format(options.getProject(), options.getOutput());

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

        pipeline.run().waitUntilFinish();
    }

}