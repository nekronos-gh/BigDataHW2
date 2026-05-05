#!/bin/sh

# Find the first JAR
JAR=$(find target -name '*.jar' 2>/dev/null | head -n 1)

if [ -z "$JAR" ]; then
	echo "JAR not found! Build failed."
	exit 1
fi

# Memory: use what job.slurm exported, or fall back to a safe default
DRIVER_MEMORY="${DRIVER_MEMORY:-50g}"

echo "Running code with driver memory: ${DRIVER_MEMORY}"
"$SPARK_HOME"/bin/spark-submit \
	--master local[*] \
	--driver-memory "${DRIVER_MEMORY}" \
	--executor-memory "${DRIVER_MEMORY}" \
	--conf spark.driver.maxResultSize=8g \
	--conf spark.memory.fraction=0.8 \
	--conf spark.memory.storageFraction=0.3 \
	--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	--conf spark.kryoserializer.buffer.max=512m \
	--conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
	--conf spark.driver.extraJavaOptions="-XX:+UseG1GC" \
	--class Problem3 \
	"$JAR"
