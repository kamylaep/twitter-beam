package com.kep.beam.pubsub.countwords;

import java.util.Arrays;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.kep.beam.pubsub.countwords.PubSubCountWordsTwitterBean.ProcessTweet;
import com.kep.beam.pubsub.options.PubSubBeamOptions;

public class PubSubCountWordsTwitterBeanTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.fromOptions(buildOptions());

    @Test
    public void shouldCountTweetWords() {
        PubSubBeamOptions options = testPipeline.getOptions().as(PubSubBeamOptions.class);
        PCollection<String> output = testPipeline
            .apply("Create input", Create.of(Arrays.asList("{'text':'I am a tweet tweet'}")))
            .apply("ProcessTweet", new ProcessTweet(options.getWindowInSeconds()));
        PAssert.that(output).containsInAnyOrder("I:1", "am:1", "a:1", "tweet:2");
        testPipeline.run().waitUntilFinish();
    }

    private PubSubBeamOptions buildOptions() {
        PubSubBeamOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(PubSubBeamOptions.class);
        pipelineOptions.setWindowInSeconds(10);
        return pipelineOptions;
    }

}
