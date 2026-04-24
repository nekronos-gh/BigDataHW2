#!/bin/bash
#SBATCH --job-name=p3
#SBATCH --output=p3_%j.out
#SBATCH --error=p3_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=16G
#SBATCH --time=02:00:00
#SBATCH --partition=batch

# Load required modules
module load lang/Java/21.0.7
module load devel/Spark/3.5.4-foss-2023b-Java-17

# Set SBT version
SBT_VERSION=1.10.1
SBT_HOME=/opt/sbt

# Install SBT if not exists
if [ ! -d "$SBT_HOME" ]; then
  wget -q "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz"
  tar -xzf sbt-${SBT_VERSION}.tgz -C /opt
  rm sbt-${SBT_VERSION}.tgz
fi
export PATH="${SBT_HOME}/bin:$PATH"

# Build the project
sbt package

# Run the project
./run.sh
