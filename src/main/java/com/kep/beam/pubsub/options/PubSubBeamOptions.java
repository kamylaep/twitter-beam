package com.kep.beam.pubsub.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;

public interface PubSubBeamOptions extends GcpOptions, PipelineOptions {

  long getWindowInSeconds();

  void setWindowInSeconds(long windowInSeconds);

  int getWriteShards();

  void setWriteShards(int writeShards);

  String getUserInput();

  void setUserInput(String userInput);

  String getTweetInput();

  void setTweetInput(String tweetInput);

  String getOutput();

  void setOutput(String output);

  Integer getFollowersCount();

  void setFollowersCount(Integer followersCount);

  String getTweetSource();

  void setTweetSource(String tweetSource);

}
