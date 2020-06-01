package com.kep.beam.pubsub.followers;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.gson.Gson;
import com.kep.beam.pubsub.followers.PubSubFilterFollowersTwitterBean.ProcessTwitter;
import com.kep.beam.pubsub.options.PubSubBeamOptions;

public class PubSubFilterFollowersTwitterBeanTest {

    public static final int FOLLOWERS_COUNT = 200;
    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.fromOptions(buildOptions());

    @Test
    public void shouldCountTweetWords() {
        PubSubBeamOptions options = testPipeline.getOptions().as(PubSubBeamOptions.class);
        List<String> inputData = createInputData();
        PCollection<String> output = testPipeline
            .apply("Create input", Create.of(inputData))
            .apply("ProcessTweet", new ProcessTwitter(options.getWindowInSeconds()));
        PAssert.that(output).containsInAnyOrder(filterExpectedData(inputData));
        testPipeline.run().waitUntilFinish();
    }

    private PubSubBeamOptions buildOptions() {
        PubSubBeamOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(PubSubBeamOptions.class);
        pipelineOptions.setWindowInSeconds(10);
        pipelineOptions.setFollowersCount(FOLLOWERS_COUNT);
        return pipelineOptions;
    }

    private List<String> createInputData() {
        Gson gson = new Gson();
        return IntStream.range(0, 10)
            .mapToObj(i -> {
                Map<String, String> userData = new TreeMap<>();
                userData.put("user.id", randomNumeric(15));
                userData.put("user.name", randomAlphabetic(30));
                userData.put("user.screen_name", randomAlphabetic(10));
                userData.put("user.friends_count", "11");
                userData.put("user.followers_count", i % 2 == 0 ? "201" : "50");
                return userData;
            })
            .map(gson::toJson)
            .collect(Collectors.toList());
    }

    private List<String> filterExpectedData(List<String> input) {
        Gson gson = new Gson();
        return input.stream()
            .map(a -> gson.fromJson(a, Map.class))
            .filter(m -> Long.parseLong(m.get("user.followers_count").toString()) > FOLLOWERS_COUNT)
            .map(m -> "{\"user.id\":\"" + m.get("user.id") + "\",\"user.followers_count\":\"" + m.get("user.followers_count")
                + "\",\"user.screen_name\":\"" + m.get("user.screen_name") + "\"}")
            .collect(Collectors.toList());
    }

}
