#!/bin/sh

scp -i ~/.ssh/google-cloud-cs123 pickle_pairs.py emo@104.198.254.24:~/  # Master
scp -i ~/.ssh/google-cloud-cs123 pickle_pairs.py emo@104.154.250.54:~/  # Slave1
