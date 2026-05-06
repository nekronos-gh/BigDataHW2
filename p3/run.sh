#!/bin/sh

JAR=$(find target -name '*.jar' 2>/dev/null | head -n 1)

if [ -z "$JAR" ]; then
  echo "JAR not found! Build failed."
  exit 1
fi

"$SPARK_HOME"/bin/spark-submit \
  --master "local[*]" \
  --driver-memory 220g \
  --driver-java-options "-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:InitiatingHeapOccupancyPercent=35" \
  --conf spark.driver.maxResultSize=20g \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.default.parallelism=256 \
  --conf spark.sql.shuffle.partitions=256 \
  --conf spark.locality.wait=0 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --class Problem3 \
  "$JAR"
