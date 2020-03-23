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
import org.apache.beam.sdk.values.PCollection;
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
    new PubSubCountWordsTwitterBean().start(args);
  }

  private void start(String[] args) {
    PipelineOptionsFactory.register(PubSubBeamOptions.class);
    PubSubBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubBeamOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    LOGGER.debug("Registering app options={}", options);
    LOGGER.debug("Building pipeline");

    PCollection<String> inputCollection = pipeline.apply("ReadTwitterTopic", PubsubIO.readStrings().fromSubscription(getSubscription(options)));

    PCollection<String> outputCollection = buildPipeline(options, inputCollection);

    outputCollection.apply("WriteData", TextIO.write()
        .to(options.getOutput())
        .withWindowedWrites()
        .withNumShards(options.getWriteShards())
        .withSuffix(".txt")
    );

    LOGGER.debug("Starting pipeline");
    pipeline.run().waitUntilFinish();
  }

  protected PCollection<String> buildPipeline(PubSubBeamOptions options, PCollection<String> inputCollection) {
    return inputCollection
        .apply("GetTweet", MapElements.into(TypeDescriptors.strings()).via((String tweetJson) -> {
          JsonObject jsonObject = new Gson().fromJson(tweetJson, JsonObject.class);
          JsonPrimitive text = jsonObject.getAsJsonPrimitive("text");
          return Optional.ofNullable(text).map(JsonPrimitive::getAsString).orElse("");
        }))
        .apply("SplitWords", FlatMapElements.into(TypeDescriptors.strings()).via(word -> Arrays.asList(word.split("[\\W\\d]"))))
        .apply("RemoveEmptyWords", Filter.by(word -> !word.isEmpty()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds()))))
        .apply("CountWords", Count.perElement())
        .apply("MapOutput", MapElements.into(TypeDescriptors.strings()).via(wordCount -> wordCount.getKey() + ":" + wordCount.getValue()));
  }

  private String getProject(PubSubBeamOptions options) {
    return "projects/" + options.getProject();
  }

  private String getSubscription(PubSubBeamOptions options) {
    return getProject(options) + "/subscriptions/" + options.getInput();
  }

}