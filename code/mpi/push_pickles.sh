#!/bin/sh

scp -i ~/.ssh/google-cloud-cs123 pickles/music0.pkl emo@104.198.254.24:~/  # Master
scp -i ~/.ssh/google-cloud-cs123 pickles/music1.pkl emo@104.198.254.24:~/  # Master
scp -i ~/.ssh/google-cloud-cs123 pickles/music0.pkl emo@104.154.250.54:~/  # Slave1
scp -i ~/.ssh/google-cloud-cs123 pickles/music1.pkl emo@104.154.250.54:~/  # Slave1
