# Twitter Beam

This project has the following examples of pipelines using Apache Bean:
1. **Followers (Kafka)**: Consumes messages from Kafka, filters users with more than X followers and writes it back to Kafka.
2. **Followers (GCP)**: Consumes messages from GCP Pub/Sub, filters users with more than X followers and writes it back to GCP Pub/Sub.
3. **Count**: Consumes messages from GCP Pub/Sub, count every word from a tweet and writes it to Google Cloud Storage.
4. **Source**: Consumes messages from two GCP Pub/Sub topics (data from users and tweets), filter all tweets from some parameterize source and writes it back to GCP Pub/Sub.

## Configuring & Running 

### Kafka pipeline

Use the [kafka-java-twitter](https://github.com/kamylaep/kafka-java-twitter) project to produce the messages.

To create the output topic, use:

```shell script
$ docker exec kafka kafka-topics --zookeeper zookeeper:2181 --topic twitter-users-with-more-than-200-followers --create --partitions 5 --replication-factor 1
```

To consume from the topic:

```shell script
$ docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic twitter-users-with-more-than-200-followers --from-beginning --property print.key=true --property print.value=true 
```

To execute the pipeline: 

```shell script
$ mvn compile exec:java -Dexec.mainClass=com.kep.beam.kafka.KafkaTwitterBean \
-Dexec.args="--kafkaBootstrapServer=localhost:9092 --input=twitter-in --output=twitter-users-with-more-than-200-followers --followersCount=200" \
-Pdirect-runner
```

### GCP pipelines

Use the [pubsub-twitter](https://github.com/kamylaep/pubsub-twitter) project to produce the messages.

To create the output topics and the subscriptions, use: 

```shell script
$ gcloud pubsub topics create twitter-users-with-more-than-200-followers
$ gcloud pubsub subscriptions create sub-twitter-users-with-more-than-200-followers --topic twitter-users-with-more-than-200-followers

$ gcloud pubsub topics create twitter-word-count
$ gcloud pubsub subscriptions create sub-twitter-word-count --topic twitter-word-count
```

To execute the pipeline:

#### Followers

##### Direct Runner

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALS=<PATH-TO-CREDENTIALS> && \
mvn compile exec:java -Dexec.mainClass=com.kep.beam.pubsub.followers.TwitterFollowersPipeline \
-Dexec.args="--project=<PROJECT_ID> --userInput=sub-twitter-user --output=twitter-users-with-more-than-200-followers --followersCount=200 --windowInSeconds=60" \
-Pdirect-runner
```

##### Dataflow runner

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALSe=<PATH-TO-CREDENTIALS> && \
mvn compile exec:java -Dexec.mainClass=com.kep.beam.pubsub.followers.TwitterFollowersPipeline \
-Dexec.args="--project=<PROJECT_ID> --userInput=sub-twitter-user --output=twitter-users-with-more-than-200-followers --followersCount=200 --windowInSeconds=60 --runner=dataflow --streaming=true" \
-Pdataflow-runner
```

#### Count

##### Direct runner

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALS=<PATH-TO-CREDENTIALS> && \
mvn compile exec:java -Dexec.mainClass=com.kep.beam.pubsub.countwords.CountWordsTweetPipeline \
-Dexec.args="--project=<PROJECT_ID> --tweetInput=sub-twitter-tweet --output=gs://twitter-count-beam/count/tweet-count --windowInSeconds=60 --writeShards=2" \
-Pdirect-runner
```

##### Dataflow runner

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALS=<PATH-TO-CREDENTIALS> && \
mvn compile exec:java -Dexec.mainClass=com.kep.beam.pubsub.countwords.CountWordsTweetPipeline \
-Dexec.args="--project=<PROJECT_ID> --tweetInput=sub-twitter-tweet --output=gs://twitter-count-beam/count/tweet-count --windowInSeconds=60 --writeShards=2 --runner=dataflow --streaming=true" \
-Pdataflow-runner
```

#### Source

##### Direct runner

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALS=<PATH-TO_CREDENTIALS> && \
mvn compile exec:java -Dexec.mainClass=com.kep.beam.pubsub.source.PubSubTwitterSourceBean \
-Dexec.args="--project=<PROJECT_ID> --tweetInput=sub-twitter-tweet --userInput=sub-twitter-user --output=twitter-from-android --tweetSource=android --windowInSeconds=60" \
-Pdirect-runner
```

##### Dataflow runner

```shell script
$ export GOOGLE_APPLICATION_CREDENTIALS=<PATH-TO_CREDENTIALS> && \
mvn compile exec:java -Dexec.mainClass=com.kep.beam.pubsub.source.PubSubTwitterSourceBean \
-Dexec.args="--project=<PROJECT_ID> --tweetInput=sub-twitter-tweet --userInput=sub-twitter-user --output=twitter-from-android --tweetSource=android --windowInSeconds=60 --runner=dataflow --streaming=true" \
-Pdataflow-runner
```