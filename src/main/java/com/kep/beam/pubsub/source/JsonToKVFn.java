package com.kep.beam.pubsub.source;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.google.gson.Gson;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class JsonToKVFn<T extends InputData> extends DoFn<String, KV<String, T>> {

    private Class<T> jsonClass;

    @ProcessElement
    public void processElement(@Element String json, OutputReceiver<KV<String, T>> outputReceiver) {
        T data = new Gson().fromJson(json, jsonClass);
        outputReceiver.output(KV.of(data.getUserId(), data));
    }
}