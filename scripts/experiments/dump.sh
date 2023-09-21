#!/usr/bin/env bash

name=$1
run=$2

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/kafka_2.13-3.1.0"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/Users/samuelelanghi/Documents/projects/kEPLr"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi

topic="$name-run-$run"
output="output-$name-run-$run"

#start dumper
echo "Starting dumper: $name"
java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.consumer.ResultDumper ${topic} ${output} ${run}
echo "dumper finished: $name"

