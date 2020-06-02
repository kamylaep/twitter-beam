package com.kep.beam.pubsub.source;

import static com.kep.beam.pubsub.source.TwitterSourcePipeline.TWEET_TAG;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.google.gson.Gson;

public class TwitterSourcePipelineTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.fromOptions(buildOptions());

    private Gson gson = new Gson();

    @Test
    public void test() {
        List<String> inputUserData = createInputUserData();
        PCollection<KV<String, Map<String, String>>> users = testPipeline
            .apply("ReadUsersTopic", Create.of(inputUserData))
            .apply("UserJsonToVK", ParDo.of(new JsonToKVFn()));

        PCollection<KV<String, Map<String, String>>> tweets = testPipeline
            .apply("ReadTweetsTopic", Create.of(createInputTweetData(inputUserData)))
            .apply("TweetJsonToKV", ParDo.of(new JsonToKVFn()));

        PCollection<String> result = KeyedPCollectionTuple.of(TwitterSourcePipeline.USER_TAG, users).and(TWEET_TAG, tweets)
            .apply("ProcessData", new ProcessData());

        PAssert.that(result)
            .containsInAnyOrder("{\"username\":\"user1\",\"tweets\":[\"text1\",\"text2\"]}",
                "{\"username\":\"user2\",\"tweets\":[\"text4\"]}");

        testPipeline.run().waitUntilFinish();
    }

    private SourceOptions buildOptions() {
        SourceOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(SourceOptions.class);
        pipelineOptions.setWindowInSeconds(10);
        pipelineOptions.setTweetSource("android");
        return pipelineOptions;
    }

    private List<String> createInputUserData() {
        Map<String, String> user1 = new TreeMap<>();
        user1.put("user.id", "1");
        user1.put("user.screen_name", "user1");

        Map<String, String> user2 = new TreeMap<>();
        user2.put("user.id", "2");
        user2.put("user.screen_name", "user2");

        Map<String, String> user3 = new TreeMap<>();
        user3.put("user.id", "3");
        user3.put("user.screen_name", "user3");

        return Arrays.asList(gson.toJson(user1), gson.toJson(user2), gson.toJson(user3));
    }

    private List<String> createInputTweetData(List<String> inputUserData) {
        Map<String, String> twee1 = new HashMap<>();
        twee1.put("user.id", "1");
        twee1.put("source", "android");
        twee1.put("text", "text1");

        Map<String, String> twee2 = new HashMap<>();
        twee2.put("user.id", "1");
        twee2.put("source", "android");
        twee2.put("text", "text2");

        Map<String, String> twee3 = new HashMap<>();
        twee3.put("user.id", "1");
        twee3.put("source", "ios");
        twee3.put("text", "text3");

        Map<String, String> twee4 = new HashMap<>();
        twee4.put("user.id", "2");
        twee4.put("source", "android");
        twee4.put("text", "text4");

        return Arrays.asList(gson.toJson(twee1), gson.toJson(twee2), gson.toJson(twee3), gson.toJson(twee4));
    }

}
