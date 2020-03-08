package com.kep.beam.pipeline;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterUsersWithMoreThanXFollowersDoFn extends DoFn<Map<String, String>, Map<String, String>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterUsersWithMoreThanXFollowersDoFn.class);

  @ProcessElement
  public void processElement(ProcessContext c) {
    LOGGER.debug("Starting filter users");

    TwitterBeanOptions options = c.getPipelineOptions().as(TwitterBeanOptions.class);
    Double followersCount = Double.parseDouble(c.element().get("followers_count"));
    if (followersCount > options.getFollowersCount()) {
      LOGGER.trace("Filtered user={}", c.element());
      c.output(c.element());
    }

    LOGGER.debug("Finishing filter users");
  }
}
