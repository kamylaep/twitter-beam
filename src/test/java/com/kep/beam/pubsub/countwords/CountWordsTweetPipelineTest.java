package com.kep.beam.pubsub.countwords;

import java.util.Arrays;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class CountWordsTweetPipelineTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.fromOptions(buildOptions());

    @Test
    public void shouldCountTweetWords() {
        CountWordsOptions options = testPipeline.getOptions().as(CountWordsOptions.class);
        PCollection<String> output = testPipeline
            .apply("Create input", Create.of(Arrays.asList("{'text':'I am a tweet tweet'}")))
            .apply("ProcessTweet", new ProcessTweet(options.getWindowInSeconds()));
        PAssert.that(output).containsInAnyOrder("I:1", "am:1", "a:1", "tweet:2");
        testPipeline.run().waitUntilFinish();
    }

    private CountWordsOptions buildOptions() {
        CountWordsOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(CountWordsOptions.class);
        pipelineOptions.setWindowInSeconds(10);
        return pipelineOptions;
    }

}
