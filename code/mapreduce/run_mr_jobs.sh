#!/bin/bash

# LEGACY
# Uses the script that passes pairs of filepaths to compute pairwise distances
#python3 distance1D_mr.py --jobconf mapreduce.job.reduces=1 pairs_trimmed.csv > temp_distances.tsv



# Make the pickle
python3 pickle_dicto.py

# Uses the script that unpickles the catalog to compute pairwise distances
#python3 pickle2distance1D_mr.py --jobconf mapreduce.job.reduces=1 pairsIDX_trimmed.csv > temp_distances.tsv

# Characteristics for individual distributions (per song)
python3 distro_mr.py --jobconf mapreduce.job.reduces=1 temp_distances.tsv > temp_distributions.tsv

# Characteristics of the overall average distribution
python3 meta_distro_mr.py --jobconf mapreduce.job.reduces=1 temp_distributions.tsv > temp_meta_distribution.tsv

# Top 10 most eclectic songs in the dataset
python3 most_eclectic_mr.py --jobconf mapreduce.job.reduces=1 temp_distributions.tsv > temp_most_eclectic.tsv

# Top 10 most formulaic songs in the dataset
python3 most_formulaic_mr.py --jobconf mapreduce.job.reduces=1 temp_distributions.tsv > temp_most_formulaic.tsv




# Finds the two most similar songs in the dataset
# Is returning two pairs of similar songs at present
python3 most_similar_mr.py --jobconf mapreduce.job.reduces=1 temp_distances.tsv > temp_most_similar.tsv



# Haven't written this last script yet
# Finds the two most dissimilar songs in the dataset
#python3 least_similar_mr.py --jobconf mapreduce.job.reduces=1 temp_distances.tsv > temp_least_similar.tsv
