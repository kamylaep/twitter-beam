package com.kep.beam.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class KafkaTwitterBean {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(KafkaBeanOptions.class);
    KafkaBeanOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaBeanOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply("ReadTwitterTopic", KafkaIO.<String, String>read()//
        .withBootstrapServers(options.getKafkaBootstrapServer())//
        .withTopic(options.getInput())//
        .withKeyDeserializer(StringDeserializer.class)//
        .withValueDeserializer(StringDeserializer.class)//
        .withoutMetadata()//
    ).apply(Values.create()//
    ).apply("GetUserData", ParDo.of(new GetUserDataDoFn())//
    ).apply("FilterUsersWithMoreThanXFollowers", ParDo.of(new FilterUsersWithMoreThanXFollowersDoFn())//
    ).apply("ParseUserToJson", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
        .via(user -> KV.of(user.get("id"), new Gson().toJson(user)))//
    ).apply(Window.into(FixedWindows.of(Duration.standardSeconds(5)))//
    ).apply("WriteUsersToTopic", KafkaIO.<String, String>write()//
        .withBootstrapServers(options.getKafkaBootstrapServer())//
        .withTopic(options.getOutput())//
        .withKeySerializer(StringSerializer.class)//
        .withValueSerializer(StringSerializer.class));

    pipeline.run().waitUntilFinish();
  }

  public static class GetUserDataDoFn extends DoFn<String, Map<String, String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetUserDataDoFn.class);

    @ProcessElement
    public void processElement(@Element String jsonTweet, OutputReceiver<Map<String, String>> outputReceiver) {
      LOGGER.debug("Starting extract user data from tweet");
      LOGGER.trace("Received tweet={}", jsonTweet);

      Gson gson = new Gson();
      JsonObject tweet = gson.fromJson(jsonTweet, JsonObject.class);
      JsonObject user = tweet.getAsJsonObject("user");

      Map<String, String> out = new HashMap<>();
      out.put("id", user.get("id").getAsString());
      out.put("screen_name", user.get("screen_name").getAsString());
      out.put("followers_count", user.get("followers_count").getAsString());
      outputReceiver.output(out);

      LOGGER.debug("Finishing extract user data from tweet");
      LOGGER.trace("Extracted data={}", out);
    }
  }

  public static class FilterUsersWithMoreThanXFollowersDoFn extends DoFn<Map<String, String>, Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterUsersWithMoreThanXFollowersDoFn.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
      LOGGER.debug("Starting filter users");

      KafkaBeanOptions options = c.getPipelineOptions().as(KafkaBeanOptions.class);
      Double followersCount = Double.parseDouble(c.element().get("followers_count"));
      if (followersCount > options.getFollowersCount()) {
        LOGGER.trace("Filtered user={}", c.element());
        c.output(c.element());
      }

      LOGGER.debug("Finishing filter users");
    }
  }
}
