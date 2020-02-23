package com.kep.beam.pipeline;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;

public class FilterUsersWithMoreThanXFollowersDoFn extends DoFn<Map<String, String>, Map<String, String>> {

  @ProcessElement
  public void processElement(ProcessContext c) {
    TwitterBeanOptions options = c.getPipelineOptions().as(TwitterBeanOptions.class);
    Double followersCount = Double.parseDouble(c.element().get("followers_count"));
    if (followersCount > options.getFollowersCount()) {
      c.output(c.element());
    }
  }
}
