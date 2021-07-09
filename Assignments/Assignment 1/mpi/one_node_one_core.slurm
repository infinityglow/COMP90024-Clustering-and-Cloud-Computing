#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=0-1:00:00
module load foss/2019b
module load python/3.7.4
python3 line2offset.py
srun -n 1 python3 tweet_mpi.py
