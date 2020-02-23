# Twitter Beam

This project has two examples of pipelines using Apache Bean:
1. Consumes messages from Kafka, filters users with more than X followers and writes it back to Kafka.
2. Consumes messages from GCP Pub/Sub, filters users with more than X followers and writes it back to GCP Pub/Sub.

## Configuration 

### Kafka

Use the [kafka-java-twitter](https://github.com/kamylaep/kafka-java-twitter) project to produce the messages.

To create the output topic, use:

```shell script
$ docker exec kafka kafka-topics --zookeeper zookeeper:2181 --topic twitter-users-with-more-than-200-followers --create --partitions 5 --replication-factor 1
```

To consume from the topic:

```shell script
$ docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic twitter-users-with-more-than-200-followers --from-beginning --property print.key=true --property print.value=true 
```

### GCP 

Use the [pubsub-twitter](https://github.com/kamylaep/pubsub-twitter) project to produce the messages.

To create the output topic and subscriptions, use:

```shell script
$ gcloud pubsub topics create twitter-users-with-more-than-200-followers
$ gcloud pubsub sugbscriptions create sub-twitter-users-with-more-than-200-followers --topic twitter-users-with-more-than-200-followers
```

## Execution

### Kafka

```shell script
$ mvn compile exec:java -Dexec.mainClass=com.kep.beam.kafka.KafkaTwitterBean \
-Dexec.args="--kafkaBootstrapServer=localhost:9092 --input=twitter-in --output=twitter-users-with-more-than-200-followers --followersCount=200" \
-Pdirect-runner
```

### GCP Pub/Sub

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALS=<path-to-credentials> && \
mvn compile exec:java -Dexec.mainClass=com.kep.beam.pubsub.PubSubTwitterBean \
-Dexec.args="--project=groovy-momentum-268319 --input=sub-twitter-in --output=twitter-users-with-more-than-200-followers --followersCount=200" \
-Pdirect-runner
```

```shell script
$ 
export GOOGLE_APPLICATION_CREDENTIALSe=<path-to-credentials> && \
mvn compile exec:java -Dexec.mainClass=com.kep.beam.pubsub.PubSubTwitterBean \
-Dexec.args="--project=groovy-momentum-268319 --input=sub-twitter-in --output=twitter-users-with-more-than-200-followers --followersCount=200 --runner=dataflow --streaming=true --gcpTempLocation=gs://twitter-dataflow-kep/temp" \
-Pdataflow-runner
```

