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

module load tools/Apptainer/1.4.0
apptainer run --bind .:/workspace p3.sif
