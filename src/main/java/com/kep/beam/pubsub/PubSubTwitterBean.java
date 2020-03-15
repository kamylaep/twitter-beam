package com.kep.beam.pubsub;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.kep.beam.pipeline.FilterUsersWithMoreThanXFollowersDoFn;
import com.kep.beam.pipeline.GetUserDataDoFn;

public class PubSubTwitterBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(PubSubTwitterBean.class);

  public static void main(String[] args) {
    PipelineOptionsFactory.register(PubSubBeamOptions.class);
    PubSubBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubBeamOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.debug("Registering app options={}", options);

    String project = "projects/" + options.getProject();
    String subscription = project + "/subscriptions/" + options.getInput();
    String topicOut = project + "/topics/" + options.getOutput();

    LOGGER.debug("Building pipeline");
    pipeline.apply("ReadTwitterTopic", PubsubIO.readStrings().fromSubscription(subscription)//
    ).apply("GetUserData", ParDo.of(new GetUserDataDoFn())//
    ).apply("FilterUsersWithMoreThanXFollowers", ParDo.of(new FilterUsersWithMoreThanXFollowersDoFn())//
    ).apply("ParseUserToJson", MapElements.into(TypeDescriptors.strings()).via(user -> new Gson().toJson(user))//
    ).apply(Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds())))//
    ).apply("WriteUsersToTopic", PubsubIO.writeStrings().to(topicOut));

    LOGGER.debug("Starting pipeline");
    pipeline.run().waitUntilFinish();
  }
}
