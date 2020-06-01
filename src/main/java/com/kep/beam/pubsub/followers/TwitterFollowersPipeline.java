package com.kep.beam.pubsub.followers;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;

public class TwitterFollowersPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterFollowersPipeline.class);

    public static void main(String[] args) {
        PipelineOptionsFactory.register(FollowersOptions.class);
        FollowersOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FollowersOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        String subscription = ProjectSubscriptionName.format(options.getProject(), options.getUserInput());
        String topicOut = ProjectTopicName.format(options.getProject(), options.getOutput());

        pipeline.apply("ReadTwitterTopic", PubsubIO.readStrings().fromSubscription(subscription))
            .apply("ProcessTwitter", new ProcessTwitter(options.getWindowInSeconds()))
            .apply("WriteUsersToTopic", PubsubIO.writeStrings().to(topicOut));

        pipeline.run().waitUntilFinish();
    }
}
