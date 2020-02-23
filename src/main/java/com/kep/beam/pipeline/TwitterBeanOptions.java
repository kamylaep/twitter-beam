package com.kep.beam.pipeline;

import org.apache.beam.sdk.options.PipelineOptions;

public interface TwitterBeanOptions extends PipelineOptions {

  String getInput();

  void setInput(String input);

  String getOutput();

  void setOutput(String output);

  Integer getFollowersCount();

  void setFollowersCount(Integer followersCount);
}
