package com.kep.beam.kafka;

import com.kep.beam.pipeline.TwitterBeanOptions;

public interface KafkaBeanOptions extends TwitterBeanOptions {

  String getKafkaBootstrapServer();

  void setKafkaBootstrapServer(String kafkaBootstrapServer);

}
