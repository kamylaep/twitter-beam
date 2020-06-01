package com.kep.beam.pubsub.followers;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;

public class FilterUsersWithMoreThanXFollowersFn extends DoFn<Map<String, String>, Map<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) {

        FollowersOptions options = c.getPipelineOptions().as(FollowersOptions.class);
        Double followersCount = Double.parseDouble(c.element().get("user.followers_count"));
        if (followersCount >= options.getFollowersCount()) {
            c.output(c.element());
        }

    }
}