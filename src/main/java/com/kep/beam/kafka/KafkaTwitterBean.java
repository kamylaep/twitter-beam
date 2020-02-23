package com.kep.beam.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import com.google.gson.Gson;
import com.kep.beam.pipeline.FilterUsersWithMoreThanXFollowersDoFn;
import com.kep.beam.pipeline.GetUserDataDoFn;

public class KafkaTwitterBean {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(KafkaBeanOptions.class);
    KafkaBeanOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaBeanOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline.apply("ReadTwitterTopic", KafkaIO.<String, String>read()//
        .withBootstrapServers(options.getKafkaBootstrapServer())//
        .withTopic(options.getInput())//
        .withKeyDeserializer(StringDeserializer.class)//
        .withValueDeserializer(StringDeserializer.class)//
        .withoutMetadata()//
    ).apply(Values.create()//
    ).apply("GetUserData", ParDo.of(new GetUserDataDoFn())//
    ).apply("FilterUsersWithMoreThanXFollowers", ParDo.of(new FilterUsersWithMoreThanXFollowersDoFn())//
    ).apply("ParseUserToJson", MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
        .via(user -> KV.of(user.get("id"), new Gson().toJson(user)))//
    ).apply(Window.into(FixedWindows.of(Duration.standardSeconds(5)))//
    ).apply("WriteUsersToTopic", KafkaIO.<String, String>write()//
        .withBootstrapServers(options.getKafkaBootstrapServer())//
        .withTopic(options.getOutput())//
        .withKeySerializer(StringSerializer.class)//
        .withValueSerializer(StringSerializer.class));

    pipeline.run().waitUntilFinish();
  }
}
