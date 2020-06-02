package com.kep.beam.pubsub.source;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.gson.Gson;

public class JsonToKVFn extends DoFn<String, KV<String, Map<String, String>>> {

    @ProcessElement
    public void processElement(@Element String json, OutputReceiver<KV<String, Map<String, String>>> outputReceiver) {
        Map<String, String> map = new Gson().fromJson(json, Map.class);
        outputReceiver.output(KV.of(map.get("user.id"), map));
    }
}