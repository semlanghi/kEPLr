#!/usr/bin/env bash

#Set up Env variables
export _JAVA_OPTIONS="-Xmx10g"

if [ -z "$KAFKA_HOME" ]
then
      KAFKA_HOME="/Users/samuelelanghi/Documents/platforms/kafka_2.13-3.1.0"
else
      echo "KAFKA_HOME is set to $KAFKA_HOME"
fi

#if [ -z "$PROJECT_DIR" ]
#then
#      PROJECT_DIR="/Users/samuelelanghi/Documents/projects/kEPLr"
#else
#      echo "PROJECT_DIR is $PROJECT_DIR"
#fi

chunk_sizes=(100 500)
chunk_numbers=(10 20)
partitions_counts=(1)

# Optional Fields
broker="localhost:9092"
chunk_growth=0
mode="all"
name="all"
runs=1
dump=false

# read arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --help)
            echo "
            --broker: broker address, default 'localhost:9092'
            --name: experiment name, 'W1', 'W2', 'W3', 'W4', or 'all' for running all of them, default 'all'
            --dump: starts a consumer that dumps the result of a given experiment
            --mode: 'streams', 'esper', or 'all', default 'all'"
            exit
            ;;
        --broker)
            broker=$2
            shift 2
            ;;
        --dump)
            dump=true
            shift 1
            ;;
        --name)
            name=$2
            shift 2
            ;;
        --runs)
            runs=$2
            shift 2
            ;;
        --mode)
            mode=$2
            shift 2
            ;;
        *)
            echo "There are some wrong Options."
            exit 1
            break
            ;;
    esac
done

if [ "${name}" = "all" ]; then
  experiments=("W1" "W2" "W3" "W4")
else
  experiments=("${name}")
fi

for experiment in "${experiments[@]}"; do
  for chunk_number in "${chunk_numbers[@]}"; do
    for chunk_size in "${chunk_sizes[@]}"; do
      for partitions_count in "${partitions_counts[@]}"; do
        for ((run=0; run<${runs}; run++)) do
          topic="topic-${experiment}-chunkn-$chunk_number-chunks-$chunk_size-chunkg-$chunk_growth-pcount-$partitions_count"
          output="output-topic-${experiment}-chunkn-$chunk_number-chunks-$chunk_size-chunkg-$chunk_growth-pcount-$partitions_count"

          if [ "${mode}" = "streams" ] || [ "${mode}" = "all" ]; then
              echo "Starting Kafka Streams application instance for $name"
              echo java -cp ../../target/keplr-jar-with-dependencies.jar evaluation.keplr.WorkerMain --broker ${broker} --name ${experiment} --within ${chunk_size} --run "${run}" --partitions-count ${partitions_count} --topic "${topic}" --output "${output}"
              java -cp ../../target/keplr-jar-with-dependencies.jar evaluation.keplr.WorkerMain --broker ${broker} --name ${experiment} --within ${chunk_size} --run "${run}" --partitions-count ${partitions_count} --topic "${topic}" --output "${output}" &> "./outputs/${name}-run-${run}.out" &

              if [ ${dump} = true ]; then
                  echo "Starting consumer instance on topic ${output}."
                  java -cp ../../target/keplr-jar-with-dependencies.jar evaluation.consumer.ResultDumper ${topic} ${output} ${run} &> "./outputs/result-${name}-run-${run}".out &
              fi
              wait
          fi

          if [ "${mode}" = "esper" ] || [ "${mode}" = "all" ]; then
              echo "Starting Esper application instance for $name"
              echo java -cp ../../target/keplr-jar-with-dependencies.jar evaluation.esper.EsperEnvironment --broker ${broker} --name ${experiment} --within ${chunk_size} --run "${run}" --partitions-count ${partitions_count} --topic "${topic}" --output "${output}"
              java -cp ../../target/keplr-jar-with-dependencies.jar evaluation.esper.EsperEnvironment --broker ${broker} --name ${experiment} --within ${chunk_size} --run "${run}" --partitions-count ${partitions_count} --topic "${topic}" --output "${output}" &> "./outputs/esper-${name}-run-${run}.out" &

              if [ ${dump} = true ]; then
                  echo "Starting consumer instance on topic ${output}."
                  java -cp ../../target/keplr-jar-with-dependencies.jar evaluation.consumer.ResultDumper ${topic} ${output} ${run} &> "./outputs/result-${name}-run-${run}".out &
              fi
              wait
          fi
        done
      done
    done
  done
done





