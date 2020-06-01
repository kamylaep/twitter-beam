package com.kep.beam.pubsub.followers;

import java.util.Map;
import java.util.TreeMap;

import org.apache.beam.sdk.transforms.DoFn;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class GetUserDataFn  extends DoFn<String, Map<String, String>> {

    @ProcessElement
    public void processElement(@Element String jsonTweet, OutputReceiver<Map<String, String>> outputReceiver) {

        Gson gson = new Gson();
        JsonObject user = gson.fromJson(jsonTweet, JsonObject.class);

        Map<String, String> out = new TreeMap<>();
        out.put("user.id", user.get("user.id").getAsString());
        out.put("user.screen_name", user.get("user.screen_name").getAsString());
        out.put("user.followers_count", user.get("user.followers_count").getAsString());
        outputReceiver.output(out);
    }
}
