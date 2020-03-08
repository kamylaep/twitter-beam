package com.kep.beam.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;

public class GetUserDataDoFnTest {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void getUserDatWithSuccess() throws Exception {
    PCollection<Map<String, String>> out = testPipeline
        .apply("Create input", Create.of(readTweet()))
        .apply("Get user data", ParDo.of(new GetUserDataDoFn()));

    PAssert.that(out).containsInAnyOrder(buildExpectedUser());
    testPipeline.run().waitUntilFinish();
  }

  private String readTweet() throws IOException {
    InputStream tweetIn = ClassLoader.getSystemResourceAsStream("tweet.json");
    return IOUtils.toString(tweetIn, "UTF-8");
  }

  private Map<String, String> buildExpectedUser() {
    Map<String, String> expectedUser = new HashMap<>();
    expectedUser.put("id", "1167123147264581634");
    expectedUser.put("screen_name", "delfiiriios_");
    expectedUser.put("followers_count", "61");
    return expectedUser;
  }

}
