package com.kep.beam.pubsub.source;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;

public interface SourceOptions extends GcpOptions, PipelineOptions {

  long getWindowInSeconds();

  void setWindowInSeconds(long windowInSeconds);

  String getUserInput();

  void setUserInput(String userInput);

  String getTweetInput();

  void setTweetInput(String tweetInput);

  String getOutput();

  void setOutput(String output);

  String getTweetSource();

  void setTweetSource(String tweetSource);

}
