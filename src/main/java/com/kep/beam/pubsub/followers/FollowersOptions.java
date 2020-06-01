package com.kep.beam.pubsub.followers;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

public interface FollowersOptions extends GcpOptions {

  long getWindowInSeconds();

  void setWindowInSeconds(long windowInSeconds);

  String getUserInput();

  void setUserInput(String userInput);

  String getOutput();

  void setOutput(String output);

  Integer getFollowersCount();

  void setFollowersCount(Integer followersCount);

}
