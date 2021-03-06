#!/usr/bin/env bash


export _JAVA_OPTIONS="-Xmx10g"
experiment=$1
broker_count=$2
echo $broker_count
echo $experiment
init_chunk_size=$3
nr_of_chunks=$4
chunk_growth=$5
within=5000
RUN=$6

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


#clean logs:
echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;
rm -rf /tmp/kafka-logs*;

#start zookeeper
echo "Starting zookeeper"
$KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties & sleep 10

#start brokers
echo "Starting brokers"
END=$((broker_count-2))
echo $END
for i in $(seq 0 $END)
  do
    $KAFKA_HOME/bin/kafka-server-start $PROJECT_DIR/configs/server-$i.properties &
  done
$KAFKA_HOME/bin/kafka-server-start $PROJECT_DIR/configs/server-$((broker_count-1)).properties & sleep 15

# start producers
# Setup topic
echo "Setting up producer topic"
$KAFKA_HOME/bin/kafka-topics --zookeeper localhost:2181 --delete --topic "$experiment" --if-exists
$KAFKA_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions "$broker_count" --topic "$experiment"
$KAFKA_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions "$broker_count" --topic "output_$experiment"


echo "Starting application instance $i for $experiment"
nohup java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.keplr.${experiment} ${experiment} ${broker_count} ${init_chunk_size} ${nr_of_chunks} ${chunk_growth} ${within} ${RUN}  &> KSA.out &
sleep 10
  
# Execute producer
for i in $(seq 0 $((broker_count-1)))
do
  echo "Starting producer: $experiment"
  nohup java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.producer.${experiment}Producer ${experiment} ${broker_count} ${init_chunk_size} $((nr_of_chunks*broker_count)) ${chunk_growth} ${within} $((i+6)) 0&> Producer${i}.out &
  echo "Producer finished"
done
