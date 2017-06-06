#!/bin/bash


# Characteristics for individual distributions (per song)
python3 distro_mr.py --jobconf mapreduce.job.reduces=1 all_distances.tsv > FULL_distributions_MPI.tsv

# Characteristics of the overall average distribution
python3 meta_distro_mr.py --jobconf mapreduce.job.reduces=1 FULL_distributions_MPI.tsv > FULL_meta_distribution_MPI.tsv

# Top 10 most eclectic songs in the dataset
python3 most_eclectic_mr.py --jobconf mapreduce.job.reduces=1 FULL_distributions_MPI.tsv > FULL_most_eclectic_MPI.tsv

# Top 10 most formulaic songs in the dataset
python3 most_formulaic_mr.py --jobconf mapreduce.job.reduces=1 FULL_distributions_MPI.tsv > FULL_most_formulaic_MPI.tsv

# Finds the two most similar songs in the dataset
python3 most_similar_mr.py --jobconf mapreduce.job.reduces=1 all_distances.tsv > FULL_most_similar_MPI.tsv

# Finds the two most dissimilar songs in the dataset
python3 least_similar_mr.py --jobconf mapreduce.job.reduces=1 all_distances.tsv > FULL_least_similar_MPI.tsv
