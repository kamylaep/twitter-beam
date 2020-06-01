package com.kep.beam.pubsub.countwords;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import com.google.pubsub.v1.ProjectSubscriptionName;

public class CountWordsTweetPipeline {

    public static void main(String[] args) {
        PipelineOptionsFactory.register(CountWordsOptions.class);
        CountWordsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CountWordsOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        run(options, pipeline);
    }

    public static void run(CountWordsOptions options, Pipeline pipeline) {
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

    private static String getSubscription(CountWordsOptions options) {
        return ProjectSubscriptionName.format(options.getProject(), options.getTweetInput());
    }

}