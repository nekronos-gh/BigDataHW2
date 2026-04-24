#!/bin/bash -l
#SBATCH --job-name=p3
#SBATCH --output=p3_%j.out
#SBATCH --error=p3_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --partition=batch

# Versions
SBT_VERSION=1.10.1
SBT_HOME="$HOME/sbt"

# Load modules (now works with -l)
module load devel/Spark/3.5.4-foss-2023b || true  # Optional, loads Java

# Install SBT
if [ ! -d "$SBT_HOME" ]; then
  cd /tmp  # Safe temp dir
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

# Run
sbt package
./run.sh
