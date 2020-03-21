broker_count=$1
echo $broker_count
experiment=$2
echo $experiment
chunk_size=5000
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
find /tmp -name 'kafka-log*' -exec rm -rf {} \;
find /tmp -name 'zookeper' -exec rm -rf {} \;

#start zookeeper
echo "Starting zookeeper"
$KAFKA_HOME/bin/zookeeper-server-start $KAFKA_HOME/etc/kafka/zookeeper.properties & sleep 20

#start brokers
echo "Starting brokers"
END=$((broker_count-2))
echo $END
for i in $(seq 0 $END)
  do
    $KAFKA_HOME/bin/kafka-server-start $PROJECT_DIR/configs/server-$i.properties &
  done
$KAFKA_HOME/bin/kafka-server-start $PROJECT_DIR/configs/server-$((broker_count-1)).properties & sleep 30

# start producers
# Setup topic
echo "Setting up producer topic"
$KAFKA_HOME/bin/kafka-topics --zookeeper localhost:2181 --delete --topic "$experiment" --if-exists
$KAFKA_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions "$broker_count" --topic "$experiment"

# Execute producer
echo "Starting producer: $experiment"
java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.producer.${experiment}Producer "$experiment" "$broker_count" $chunk_size
echo "Producer finished"
sleep 20

#start worker
echo "Starting worker: $experiment"
java -cp $PROJECT_DIR/target/keplr-jar-with-dependencies.jar evaluation.keplr.$experiment $broker_count
echo "Worker finished: $experiment"
sleep 20

# stop brokers
echo "Stopping brokers"
for i in $(seq 0 $END)
  do
    $KAFKA_HOME/bin/kafka-server-stop $PROJECT_DIR/configs/server-$i.properties &
  done
$KAFKA_HOME/bin/kafka-server-stop $PROJECT_DIR/configs/server-$((broker_count-1)).properties & sleep 30

# stop zookeeper
echo "Stopping zookeeper"
$KAFKA_HOME/bin/zookeeper-server-stop $KAFKA_HOME/etc/kafka/zookeeper.properties
