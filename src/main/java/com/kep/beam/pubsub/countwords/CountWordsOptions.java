package com.kep.beam.pubsub.countwords;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

public interface CountWordsOptions extends GcpOptions {

  long getWindowInSeconds();

  void setWindowInSeconds(long windowInSeconds);

  int getWriteShards();

  void setWriteShards(int writeShards);

  String getTweetInput();

  void setTweetInput(String tweetInput);

  String getOutput();

  void setOutput(String output);

}
