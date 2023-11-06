
export _JAVA_OPTIONS="-Xmx5g"

# Environment Variable Check
if [ -z "$KAFKA_HOME" ]
then
      echo "You need to set environment variable KAFKA_HOME with the kafka folder (confluent folder is NOT supported)."
      exit
fi

broker_count=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)
            echo "
            --broker-count: how many brokers to start, max is 3"
            exit
            ;;
        --broker-count)
            broker_count=$2
            shift 2
            ;;
        *)
            echo "There are some wrong Options."
            exit 1
            break
            ;;
    esac
done



#clean logs:
echo "Cleaning /kafka_log* and zookeeper folders from /tmp"
rm -rf /tmp/zookeeper;
rm -rf /tmp/kafka-logs*;

#start zookeeper if necessary
echo "Starting Zookeeper"
echo $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &> "./outputs/zoo.out" & sleep 5


#start brokers
echo "Starting brokers"
for ((i=0; i<${broker_count}; i++))
do
  echo $KAFKA_HOME/bin/kafka-server-start.sh "../../configs/server-$i.properties"
  $KAFKA_HOME/bin/kafka-server-start.sh "../../configs/server-$i.properties" &> "./outputs/kafka-$i.out" &
done

