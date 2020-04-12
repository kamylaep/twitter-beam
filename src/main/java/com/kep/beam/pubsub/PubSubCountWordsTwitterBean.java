package com.kep.beam.pubsub;

import java.util.Arrays;
import java.util.Optional;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import lombok.AllArgsConstructor;

public class PubSubCountWordsTwitterBean {

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PubSubBeamOptions.class);
        PubSubBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubBeamOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("ReadTwitterTopic", PubsubIO.readStrings().fromSubscription(getSubscription(options)))
            .apply("ProcessTweet", new ProcessTweet(options.getWindowInSeconds()))
            .apply("WriteData", TextIO.write()
                .to(options.getOutput())
                .withWindowedWrites()
                .withNumShards(options.getWriteShards())
                .withSuffix(".txt")
            );

        pipeline.run().waitUntilFinish();
    }

    private static String getProject(PubSubBeamOptions options) {
        return "projects/" + options.getProject();
    }

    private static String getSubscription(PubSubBeamOptions options) {
        return getProject(options) + "/subscriptions/" + options.getTweetInput();
    }

    @AllArgsConstructor
    public static class ProcessTweet extends PTransform<PCollection<String>, PCollection<String>> {

        private long windowInSeconds;

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            return input
                .apply("GetTweet", MapElements.into(TypeDescriptors.strings()).via((String tweetJson) -> {
                    JsonObject jsonObject = new Gson().fromJson(tweetJson, JsonObject.class);
                    JsonPrimitive text = jsonObject.getAsJsonPrimitive("text");
                    return Optional.ofNullable(text).map(JsonPrimitive::getAsString).orElse("");
                }))
                .apply("SplitWords", FlatMapElements.into(TypeDescriptors.strings()).via(word -> Arrays.asList(word.split("[\\W\\d]"))))
                .apply("RemoveEmptyWords", Filter.by(word -> !word.isEmpty()))
                .apply("TweetWindow", Window.into(FixedWindows.of(Duration.standardSeconds(windowInSeconds))))
                .apply("CountWords", Count.perElement())
                .apply("MapOutput", MapElements.into(TypeDescriptors.strings()).via(wordCount -> wordCount.getKey() + ":" + wordCount.getValue()));
        }
    }

}