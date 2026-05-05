#!/bin/bash -l
#SBATCH --job-name=rf_age_p1
#SBATCH --output=rf_age_%j.out
#SBATCH --error=rf_age_%j.err
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G

cd /home/users/mmiszkiewicz/BIGDATA/BigDataHW2

source /etc/profile

module purge
module load devel/Spark/3.5.4-foss-2023b-Java-17

which spark-submit
spark-submit --version

spark-submit \
  --driver-memory 12g \
  --conf spark.driver.maxResultSize=4g \
  Problem1_rf_age_full.py heart_2020_cleaned.csv