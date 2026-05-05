#!/bin/bash -l
#SBATCH --job-name=p3
#SBATCH --output=p3_%j.out
#SBATCH --error=p3_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=0
#SBATCH --time=04:00:00
#SBATCH --partition=batch
#SBATCH --exclusive              # Guarantee all 64 cores, no node sharing

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

# Install SBT once into $HOME (skipped on subsequent runs)
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
export SBT_OPTS="-Dsbt.global.base=$HOME/.sbt -Dsbt.ivy.home=$HOME/.ivy2 -Dsbt.boot.directory=$HOME/.sbt/boot"

# Build (only rebuilds when sources changed thanks to sbt incremental compiler)
echo "Building project..."
sbt clean package
if [ $? -ne 0 ]; then
  echo "ERROR: sbt build failed. Aborting." >&2
  exit 1
fi

# Pass memory budget to run.sh via environment variable
export DRIVER_MEMORY="100g"

# Run
echo "Submitting Spark job..."
./run.sh
