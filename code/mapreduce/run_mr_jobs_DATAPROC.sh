#!/bin/bash

# Make the pickle
#python3 pickle_dicto.py

# Uses the script that unpickles the catalog to compute pairwise distances
# TESTING on 100K sample of comparisons
#python3 pickle2distance1D_mr.py --jobconf mapreduce.job.reduces=1 pairsIDX_trimmed.csv > test_distances.tsv

# Running the full 10K songs comparisons
#python3 pickle2distance1D_mr.py --jobconf mapreduce.job.reduces=1 pairsIDX.csv > distances_10ksongs.tsv

# Characteristics for individual distributions (per song)
python3 distro_mr.py --jobconf mapreduce.job.reduces=1 distances_10ksongs.tsv > distributions_10ksongs.tsv

# Characteristics of the overall average distribution
python3 meta_distro_mr.py --jobconf mapreduce.job.reduces=1 distributions_10ksongs.tsv > meta_distribution_10ksongs.tsv

# Top 10 most eclectic songs in the dataset
python3 most_eclectic_mr.py --jobconf mapreduce.job.reduces=1 distributions_10ksongs.tsv > most_eclectic_10Ksongs.tsv

# Top 10 most formulaic songs in the dataset
python3 most_formulaic_mr.py --jobconf mapreduce.job.reduces=1 distributions_10ksongs.tsv > most_formulaic_10ksongs.tsv

# Finds the two most similar songs in the dataset
python3 most_similar_mr.py --jobconf mapreduce.job.reduces=1 distances_10ksongs.tsv > most_similar_10ksongs.tsv

# Finds the two most dissimilar songs in the dataset
python3 least_similar_mr.py --jobconf mapreduce.job.reduces=1 distances_10ksongs.tsv > least_similar_10ksongs.tsv
