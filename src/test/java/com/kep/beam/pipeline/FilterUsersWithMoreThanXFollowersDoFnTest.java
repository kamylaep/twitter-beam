package com.kep.beam.pipeline;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FilterUsersWithMoreThanXFollowersDoFnTest {

  private static final int FOLLOWERS_COUNT = 100;

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.fromOptions(buildOptions());

  @Test
  public void filterUsersWithMoreThan100Followers() {
    List<Map<String, String>> users = buildInputUser();
    PCollection<Map<String, String>> out = testPipeline
        .apply("Create input", Create.of(users).withCoder(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
        .apply("Get user data", ParDo.of(new FilterUsersWithMoreThanXFollowersDoFn()));

    PAssert.that(out).containsInAnyOrder(buildExpectedUsers(users));
    testPipeline.run().waitUntilFinish();
  }

  private PipelineOptions buildOptions() {
    TwitterBeanOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(TwitterBeanOptions.class);
    pipelineOptions.setFollowersCount(FOLLOWERS_COUNT);
    return pipelineOptions;
  }

  private List<Map<String, String>> buildInputUser() {
    return IntStream.range(0, 10)
        .mapToObj(i -> {
          Map<String, String> user = new HashMap<>();
          user.put("id", randomNumeric(19));
          user.put("screen_name", randomAlphabetic(25));
          int followers = ThreadLocalRandom.current().nextInt(i % 2 == 0 ? 99 : 300);
          user.put("followers_count", Integer.toString(followers));
          return user;
        })
        .collect(Collectors.toList());
  }

  private List<Map<String, String>> buildExpectedUsers(List<Map<String, String>> users) {
    return users.stream()
        .filter(p -> Integer.parseInt(p.get("followers_count")) > FOLLOWERS_COUNT)
        .collect(Collectors.toList());
  }

}
