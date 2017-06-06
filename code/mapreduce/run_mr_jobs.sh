#!/bin/bash

# LEGACY
# Uses the script that passes pairs of filepaths to compute pairwise distances
#python3 distance1D_mr.py --jobconf mapreduce.job.reduces=1 pairs_trimmed.csv > FULL_distances.tsv



# Make the pickle
#python3 pickle_dicto.py

# Uses the script that unpickles the catalog to compute pairwise distances
python3 pickle2distance1D_mr.py --jobconf mapreduce.job.reduces=1 pairsIDX.csv > FULL_distances.tsv

# Characteristics for individual distributions (per song)
python3 distro_mr.py --jobconf mapreduce.job.reduces=1 FULL_distances.tsv > FULL_distributions.tsv

# Characteristics of the overall average distribution
python3 meta_distro_mr.py --jobconf mapreduce.job.reduces=1 FULL_distributions.tsv > FULL_meta_distribution.tsv

# Top 10 most eclectic songs in the dataset
python3 most_eclectic_mr.py --jobconf mapreduce.job.reduces=1 FULL_distributions.tsv > FULL_most_eclectic.tsv

# Top 10 most formulaic songs in the dataset
python3 most_formulaic_mr.py --jobconf mapreduce.job.reduces=1 FULL_distributions.tsv > FULL_most_formulaic.tsv

# Finds the two most similar songs in the dataset
python3 most_similar_mr.py --jobconf mapreduce.job.reduces=1 FULL_distances.tsv > FULL_most_similar.tsv

# Finds the two most dissimilar songs in the dataset
python3 least_similar_mr.py --jobconf mapreduce.job.reduces=1 FULL_distances.tsv > FULL_least_similar.tsv
