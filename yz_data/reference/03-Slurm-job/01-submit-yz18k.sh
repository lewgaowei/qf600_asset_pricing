#!/bin/bash

cd Slurm

sbatch 01-V4_yz18k_brt.slurm
sbatch 01-V4_yz18k_nn1.slurm
sbatch 01-V4_yz18k_nn2.slurm
sbatch 01-V4_yz18k_nn3.slurm
sbatch 01-V4_yz18k_nn4.slurm
sbatch 01-V4_yz18k_nn5.slurm