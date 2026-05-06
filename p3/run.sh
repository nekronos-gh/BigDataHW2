#!/bin/sh

JAR=$(find target -name '*.jar' 2>/dev/null | head -n 1)

if [ -z "$JAR" ]; then
  echo "JAR not found! Build failed."
  exit 1
fi

DRIVER_MEMORY="${DRIVER_MEMORY:-50g}"
# Use all available cores explicitly
NUM_CORES="${SLURM_CPUS_PER_TASK:-$(nproc)}"

echo "Running with driver memory: ${DRIVER_MEMORY}, cores: ${NUM_CORES}"

"$SPARK_HOME"/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 64 \
  --executor-memory 100g \
  --driver-memory 50g \
  --driver-memory "${DRIVER_MEMORY}" \
  --conf spark.driver.maxResultSize=8g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryoserializer.buffer.max=512m \
  --conf spark.default.parallelism="${NUM_CORES}" \
  --conf spark.locality.wait=0 \
  --conf spark.speculation=false \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:G1HeapRegionSize=32m" \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:G1HeapRegionSize=32m" \
  --conf spark.ui.enabled=false \
  --class Problem3 \
  "$JAR"
