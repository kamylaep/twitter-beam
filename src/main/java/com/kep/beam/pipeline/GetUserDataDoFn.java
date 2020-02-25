package com.kep.beam.pipeline;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class GetUserDataDoFn extends DoFn<String, Map<String, String>> {

  @ProcessElement
  public void processElement(@Element String jsonTweet, OutputReceiver<Map<String, String>> outputReceiver) {
    Gson gson = new Gson();
    JsonObject tweet = gson.fromJson(jsonTweet, JsonObject.class);
    JsonObject user = tweet.getAsJsonObject("user");

    Map<String, String> out = new HashMap<>();
    out.put("id", user.get("id").getAsString());
    out.put("screen_name", user.get("screen_name").getAsString());
    out.put("followers_count", user.get("followers_count").getAsString());
    outputReceiver.output(out);
  }
}
