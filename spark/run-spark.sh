#!/bin/bash

SPARK_SUBMIT="$1"
SPARK_CLASS="$2"
shift
shift

"$SPARK_SUBMIT" \
    --master 'local[*]' \
    --driver-memory 64G \
    --executor-memory 64G \
    --files ./log4j.properties \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --class "$SPARK_CLASS" \
    target/scala-2.12/udo-benchmarks_2.12-1.0.jar "$@"
