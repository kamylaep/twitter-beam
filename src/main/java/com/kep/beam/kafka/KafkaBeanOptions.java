package com.kep.beam.kafka;

import org.apache.beam.sdk.options.PipelineOptions;

public interface KafkaBeanOptions extends PipelineOptions {

  String getKafkaBootstrapServer();

  void setKafkaBootstrapServer(String kafkaBootstrapServer);

  String getInput();

  void setInput(String input);

  String getOutput();

  void setOutput(String output);

  Integer getFollowersCount();

  void setFollowersCount(Integer followersCount);

}
