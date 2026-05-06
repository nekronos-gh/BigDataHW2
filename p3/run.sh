#!/bin/sh

JAR=$(find target -name '*.jar' 2>/dev/null | head -n 1)

if [ -z "$JAR" ]; then
  echo "JAR not found! Build failed."
  exit 1
fi

echo "Running with master in: ${MASTER_URL}"

"$SPARK_HOME"/bin/spark-submit \
  --master "${MASTER_URL}" \
  --driver-memory 40g \
  --executor-memory 60g \
  --conf spark.driver.maxResultSize=8g \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.default.parallelism=256 \
  --class Problem3 \
  "$JAR"
