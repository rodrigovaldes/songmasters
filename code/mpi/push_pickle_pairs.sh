#!/bin/sh

scp -i ~/.ssh/google-cloud-cs123 pickle_pairs-emo.py emo@104.198.254.24:~/  # Master
scp -i ~/.ssh/google-cloud-cs123 pickle_pairs-emo.py emo@104.154.250.54:~/  # Slave1







# MASTER 104.198.254.24     10.128.0.2
# SLAVE1 104.154.16.87      10.128.0.3
# SLAVE2 104.154.236.149    10.128.0.4
# SLAVE3 35.184.105.134     10.128.0.5
# SLAVE4 104.197.159.197    10.128.0.6
# SLAVE5 104.197.81.74      10.128.0.7
