package com.kep.beam.pubsub.countwords;

import java.util.Arrays;
import java.util.Optional;

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

@AllArgsConstructor
public class ProcessTweet extends PTransform<PCollection<String>, PCollection<String>> {

    private long windowInSeconds;

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input
            .apply("GetTweet", MapElements.into(TypeDescriptors.strings()).via((String tweetJson) ->
                Optional.ofNullable(new Gson().fromJson(tweetJson, JsonObject.class).getAsJsonPrimitive("text"))
                    .map(JsonPrimitive::getAsString)
                    .orElse("")))
            .apply("SplitWords", FlatMapElements.into(TypeDescriptors.strings())
                .via(word -> Arrays.asList(word.split("[\\W\\d]"))))
            .apply("RemoveEmptyWords", Filter.by(word -> !word.isEmpty()))
            .apply("TweetWindow", Window.into(FixedWindows.of(Duration.standardSeconds(windowInSeconds))))
            .apply("CountWords", Count.perElement())
            .apply("MapOutput", MapElements.into(TypeDescriptors.strings())
                .via(wordCount -> wordCount.getKey() + ":" + wordCount.getValue()));
    }
}