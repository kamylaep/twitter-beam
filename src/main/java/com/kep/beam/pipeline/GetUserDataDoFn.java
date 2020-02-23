package com.kep.beam.pipeline;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.gson.Gson;

public class GetUserDataDoFn extends DoFn<String, Map<String, String>> {

  @ProcessElement
  public void processElement(@Element String jsonTweet, OutputReceiver<Map<String, String>> outputReceiver) {
    Gson gson = new Gson();
    Map<String, Object> tweet = gson.fromJson(jsonTweet, Map.class);
    Map<String, Object> user = (Map<String, Object>) tweet.get("user");

    Map<String, String> out = new HashMap<>();
    out.put("id", user.get("id").toString());
    out.put("screen_name", user.get("screen_name").toString());
    out.put("followers_count", user.get("followers_count").toString());
    outputReceiver.output(out);
  }
}
