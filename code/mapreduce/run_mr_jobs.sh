#!/bin/bash


# Uses the script that passes pairs of filepaths to compute pairwise distances
#python3 distance1D_mr.py mapreduce.job.reduces=1 pairs_trimmed.csv > temp_distances.tsv


# Uses the script that unpickles the catalog to compute pairwise distances
#python3 pickle2distance1D_mr.py mapreduce.job.reduces=1 pairsIDX_trimmed.csv > temp_distances.tsv

# Characteristics for individual distributions (per song)
python3 distro_mr.py mapreduce.job.reduces=1 temp_distances.tsv > temp_distributions.tsv

# Characteristics of the overall average distribution
python3 meta_distro_mr.py mapreduce.job.reduces=1 temp_distributions.tsv > temp_meta_distribution.tsv

# Top 10 most eclectic songs in the dataset
python3 most_eclectic_mr.py mapreduce.job.reduces=1 temp_distributions.tsv > temp_most_eclectic.tsv

# Top 10 most formulaic songs in the dataset
python3 most_formulaic_mr.py mapreduce.job.reduces=1 temp_distributions.tsv > temp_most_formulaic.tsv




# Finds the two most similar songs in the dataset





# Finds the two most dissimilar songs in the dataset
