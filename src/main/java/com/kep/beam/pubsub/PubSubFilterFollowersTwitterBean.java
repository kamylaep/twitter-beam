package com.kep.beam.pubsub;

import java.util.Map;
import java.util.TreeMap;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
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

import lombok.AllArgsConstructor;

public class PubSubFilterFollowersTwitterBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubFilterFollowersTwitterBean.class);

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PubSubBeamOptions.class);
        PubSubBeamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubBeamOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        LOGGER.debug("Registering app options={}", options);

        String project = "projects/" + options.getProject();
        String subscription = project + "/subscriptions/" + options.getUserInput();
        String topicOut = project + "/topics/" + options.getOutput();

        LOGGER.debug("Building pipeline");
        pipeline.apply("ReadTwitterTopic", PubsubIO.readStrings().fromSubscription(subscription))
            .apply("ProcessTwitter", new ProcessTwitter(options.getWindowInSeconds()))
            .apply("WriteUsersToTopic", PubsubIO.writeStrings().to(topicOut));

        LOGGER.debug("Starting pipeline");
        pipeline.run().waitUntilFinish();
    }

    @AllArgsConstructor
    public static class ProcessTwitter extends PTransform<PCollection<String>, PCollection<String>> {

        private long windowInSeconds;

        @Override
        public PCollection<String> expand(PCollection<String> input) {
            return input
                .apply("GetUserData", ParDo.of(new GetUserDataFn()))
                .apply("FilterUsersWithMoreThanXFollowers", ParDo.of(new FilterUsersWithMoreThanXFollowersFn()))
                .apply("ParseUserToJson", MapElements.into(TypeDescriptors.strings()).via(user -> new Gson().toJson(user)))
                .apply("TwitterWindow", Window.into(FixedWindows.of(Duration.standardSeconds(windowInSeconds))));
        }
    }

    public static class GetUserDataFn extends DoFn<String, Map<String, String>> {

        private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTwitterBean.GetUserDataDoFn.class);

        @ProcessElement
        public void processElement(@Element String jsonTweet, OutputReceiver<Map<String, String>> outputReceiver) {
            LOGGER.debug("Starting extract user data from tweet");
            LOGGER.trace("Received tweet={}", jsonTweet);

            Gson gson = new Gson();
            JsonObject user = gson.fromJson(jsonTweet, JsonObject.class);

            Map<String, String> out = new TreeMap<>();
            out.put("user.id", user.get("user.id").getAsString());
            out.put("user.screen_name", user.get("user.screen_name").getAsString());
            out.put("user.followers_count", user.get("user.followers_count").getAsString());
            outputReceiver.output(out);

            LOGGER.debug("Finishing extract user data from tweet");
            LOGGER.trace("Extracted data={}", out);
        }
    }

    public static class FilterUsersWithMoreThanXFollowersFn extends DoFn<Map<String, String>, Map<String, String>> {

        private static final Logger LOGGER = LoggerFactory.getLogger(FilterUsersWithMoreThanXFollowersFn.class);

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
