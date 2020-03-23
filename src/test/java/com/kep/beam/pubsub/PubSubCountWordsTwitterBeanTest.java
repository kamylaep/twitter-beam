package com.kep.beam.pubsub;

import java.util.Arrays;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class PubSubCountWordsTwitterBeanTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.fromOptions(buildOptions());

  private PubSubCountWordsTwitterBean pubSubCountWordsTwitterBean = new PubSubCountWordsTwitterBean();

  @Test
  public void shouldCountTweetWords() {
    PCollection<String> input = testPipeline.apply("Create input", Create.of(Arrays.asList("{'text':'I am a tweet tweet'}")));
    PCollection<String> output = pubSubCountWordsTwitterBean.buildPipeline(buildOptions(), input);
    PAssert.that(output).containsInAnyOrder("I:1", "am:1", "a:1", "tweet:2");
    testPipeline.run().waitUntilFinish();
  }

  private PubSubBeamOptions buildOptions() {
    PubSubBeamOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(PubSubBeamOptions.class);
    pipelineOptions.setWindowInSeconds(10);
    return pipelineOptions;
  }

}
