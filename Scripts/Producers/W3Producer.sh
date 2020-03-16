#!/bin/sh

#Set up variables
topic="W3"
broker_count=9
chunk_size=6000

# Setup topic
/Users/tambet/confluent-5.4.0/bin/kafka-topics --zookeeper localhost:2181 --delete --topic $topic --if-exists
/Users/tambet/confluent-5.4.0/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions $broker_count --topic $topic

# Execute producer
java -cp /Users/tambet/UT/keplr/target/keplr-jar-with-dependencies.jar evaluation.producer.W3Producer $topic $broker_count $chunk_size