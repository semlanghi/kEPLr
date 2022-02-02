#!/usr/bin/env bash


export _JAVA_OPTIONS="-Xmx3g"
experiment=$1
RUN=$2

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/root/confluent-5.4.1"
else
      echo "KAFKA_HOME is $KAFKA_HOME"
fi

if [ -z "$PROJECT_DIR" ]
then
      PROJECT_DIR="/root/kEPLr"
else
      echo "PROJECT_DIR is $PROJECT_DIR"
fi

#start dumper
echo "Starting dumper: $experiment"
java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.consumer.ResultDumper $experiment $RUN
echo "dumper finished: $experiment"

