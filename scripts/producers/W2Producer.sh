#!/bin/sh

#Set up variables
topic="W2"
broker_count=$1
chunk_size=50
within=5000
growth=50

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/root/confluent-5.4.1"
else
      echo "KAFKA_HOME is set to $KAFKA_HOME"
fi
if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/Users/tambet/keplr"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi

# Setup topic
$KAFKA_HOME/bin/kafka-topics --zookeeper localhost:2181 --delete --topic $topic --if-exists
$KAFKA_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions $broker_count --topic $topic

# Execute producer
java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.producer.W2Producer $topic $broker_count $chunk_size $growth $within