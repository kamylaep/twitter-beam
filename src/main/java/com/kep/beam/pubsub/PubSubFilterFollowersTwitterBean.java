package com.kep.beam.pubsub;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.kep.beam.kafka.KafkaTwitterBean;

public class PubSubFilterFollowersTwitterBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubFilterFollowersTwitterBean.class);

  public static void main(String[] args) {
    new PubSubFilterFollowersTwitterBean().start(args);
  }

  private void start(String[] args) {
    PipelineOptionsFactory.register(PubSubBeamOptions.class);
    PubSubBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubBeamOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.debug("Registering app options={}", options);

    String project = "projects/" + options.getProject();
    String subscription = project + "/subscriptions/" + options.getInput();
    String topicOut = project + "/topics/" + options.getOutput();

    LOGGER.debug("Building pipeline");
    PCollection<String> input = pipeline.apply("ReadTwitterTopic", PubsubIO.readStrings().fromSubscription(subscription));

    PCollection<String> output = buildPipeline(options, input);

    output.apply("WriteUsersToTopic", PubsubIO.writeStrings().to(topicOut));

    LOGGER.debug("Starting pipeline");
    pipeline.run().waitUntilFinish();
  }

  protected PCollection<String> buildPipeline(PubSubBeamOptions options, PCollection<String> input) {
    return input
        .apply("GetUserData", ParDo.of(new GetUserDataDoFn()))
        .apply("FilterUsersWithMoreThanXFollowers", ParDo.of(new FilterUsersWithMoreThanXFollowersDoFn()))
        .apply("ParseUserToJson", MapElements.into(TypeDescriptors.strings()).via(user -> new Gson().toJson(user)))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds()))));
  }

  public static class GetUserDataDoFn extends DoFn<String, Map<String, String>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTwitterBean.GetUserDataDoFn.class);

    @ProcessElement
    public void processElement(@Element String jsonTweet, OutputReceiver<Map<String, String>> outputReceiver) {
      LOGGER.debug("Starting extract user data from tweet");
      LOGGER.trace("Received tweet={}", jsonTweet);

      Gson gson = new Gson();
      JsonObject user = gson.fromJson(jsonTweet, JsonObject.class);

      Map<String, String> out = new HashMap<>();
      out.put("user.id", user.get("user.id").getAsString());
      out.put("user.screen_name", user.get("user.screen_name").getAsString());
      out.put("user.followers_count", user.get("user.followers_count").getAsString());
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

      PubSubBeamOptions options = c.getPipelineOptions().as(PubSubBeamOptions.class);
      Double followersCount = Double.parseDouble(c.element().get("user.followers_count"));
      if (followersCount >= options.getFollowersCount()) {
        LOGGER.trace("Filtered user={}", c.element());
        c.output(c.element());
      }

      LOGGER.debug("Finishing filter users");
    }
  }
}
