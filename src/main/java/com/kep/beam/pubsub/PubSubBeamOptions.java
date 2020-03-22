package com.kep.beam.pubsub;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;

public interface PubSubBeamOptions extends GcpOptions, PipelineOptions {

  long getWindowInSeconds();

  void setWindowInSeconds(long windowInSeconds);

  int getWriteShards();

  void setWriteShards(int writeShards);

  String getInput();

  void setInput(String input);

  String getOutput();

  void setOutput(String output);

  Integer getFollowersCount();

  void setFollowersCount(Integer followersCount);

}
