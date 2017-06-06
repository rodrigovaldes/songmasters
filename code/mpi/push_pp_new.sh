#!/bin/sh

scp -i ~/.ssh/google-cloud-cs123 pp_new.py emo@104.198.254.24:~/   # Master
scp -i ~/.ssh/google-cloud-cs123 pp_new.py emo@104.154.16.87:~/    # Slave1
scp -i ~/.ssh/google-cloud-cs123 pp_new.py emo@104.154.236.149:~/  # Slave2
scp -i ~/.ssh/google-cloud-cs123 pp_new.py emo@35.184.105.134:~/   # Slave3
scp -i ~/.ssh/google-cloud-cs123 pp_new.py emo@104.197.159.197:~/  # Slave4
scp -i ~/.ssh/google-cloud-cs123 pp_new.py emo@104.197.81.74:~/    # Slave5
