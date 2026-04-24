#!/bin/sh

# Find the first JAR
JAR=$(find target -name '*.jar' 2>/dev/null | head -n 1)

if [ -z "$JAR" ]; then
	echo "JAR not found! Build failed."
	exit 1
fi

# Run spark-submit silently
echo "Running code!"
"$SPARK_HOME"/bin/spark-submit \
  --master local[*] \
  --driver-memory 50g \
  --class Problem3 \
  "$JAR"
