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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class PubSubCountWordsTwitterBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubCountWordsTwitterBean.class);

  public static void main(String[] args) {
    PipelineOptionsFactory.register(PubSubBeamOptions.class);
    PubSubBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubBeamOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.debug("Registering app options={}", options);

    String project = "projects/" + options.getProject();
    String subscription = project + "/subscriptions/" + options.getInput();

    LOGGER.debug("Building pipeline");

    pipeline.apply("ReadTwitterTopic", PubsubIO.readStrings().fromSubscription(subscription)
    ).apply("GetTweet", MapElements.into(TypeDescriptors.strings()).via((String tweetJson) -> {
          JsonObject jsonObject = new Gson().fromJson(tweetJson, JsonObject.class);
          JsonPrimitive text = jsonObject.getAsJsonPrimitive("text");
          return Optional.ofNullable(text).map(JsonPrimitive::getAsString).orElse("");
        })
    ).apply("SplitWords", FlatMapElements.into(TypeDescriptors.strings()).via(word -> Arrays.asList(word.split("[\\W\\d]")))
    ).apply("RemoveEmptyWords", Filter.by(word -> !word.isEmpty())
    ).apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds())))
    ).apply("CountWords", Count.perElement()
    ).apply("MapOutput", MapElements.into(TypeDescriptors.strings()).via(wordCount -> wordCount.getKey() + ":" + wordCount.getValue())
    ).apply("WriteData", TextIO.write()
        .to(options.getOutput())
        .withWindowedWrites()
        .withNumShards(options.getWriteShards())
        .withSuffix(".txt")
    );

    LOGGER.debug("Starting pipeline");
    pipeline.run().waitUntilFinish();
  }

}