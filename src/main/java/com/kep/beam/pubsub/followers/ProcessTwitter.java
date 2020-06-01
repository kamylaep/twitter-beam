package com.kep.beam.pubsub.followers;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import com.google.gson.Gson;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ProcessTwitter extends PTransform<PCollection<String>, PCollection<String>> {

    private long windowInSeconds;

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input
            .apply("GetUserData", ParDo.of(new GetUserDataFn()))
            .apply("FilterUsersWithMoreThanXFollowers", ParDo.of(new FilterUsersWithMoreThanXFollowersFn()))
            .apply("ParseUserToJson", MapElements.into(TypeDescriptors.strings()).via(user -> new Gson().toJson(user)))
            .apply("TwitterWindow", Window.into(FixedWindows.of(Duration.standardSeconds(windowInSeconds))));
    }
}
