#!/bin/bash -l
#SBATCH --job-name=p3
#SBATCH --output=p3_%j.out
#SBATCH --error=p3_%j.err
#SBATCH --nodes=10
#SBATCH --ntasks=10
#SBATCH --cpus-per-task=64
#SBATCH --mem=0
#SBATCH --time=02:00:00          # tighter deadline — job gets scheduled faster on most clusters
#SBATCH --partition=batch
#SBATCH --exclusive

# --- Versions ---
SBT_VERSION=1.10.1
SBT_HOME="$HOME/sbt"

# Load modules
module load env/development/2024a
module load devel/Spark
if [ -z "$SPARK_HOME" ]; then
  echo "ERROR: SPARK_HOME not set after module load. Aborting." >&2
  exit 1
fi

# Install SBT once into $HOME
if [ ! -d "$SBT_HOME" ]; then
  echo "Installing SBT ${SBT_VERSION}..."
  cd /tmp
  wget -q "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz"
  tar -xzf "sbt-${SBT_VERSION}.tgz"
  mv sbt "$SBT_HOME"
  rm "sbt-${SBT_VERSION}.tgz"
  cd -
fi

export PATH="${SBT_HOME}/bin:$PATH"

mkdir -p "$HOME/.runtime"
export XDG_RUNTIME_DIR="$HOME/.runtime"

# Give the JVM running sbt itself more heap
export SBT_OPTS="-Xms512m -Xmx2g \
  -Dsbt.global.base=$HOME/.sbt \
  -Dsbt.ivy.home=$HOME/.ivy2 \
  -Dsbt.boot.directory=$HOME/.sbt/boot"

# Incremental compile is much
echo "Building project..."
sbt package
if [ $? -ne 0 ]; then
  echo "ERROR: sbt build failed. Aborting." >&2
  exit 1
fi

export DRIVER_MEMORY="100g"

echo "Submitting Spark job..."
./run.sh
