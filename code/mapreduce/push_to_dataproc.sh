#scp -i ~/.ssh/google-cloud-cs123 pairsIDX_trimmed.csv emo@104.197.126.67:~/     # Slave1
scp -i ~/.ssh/google-cloud-cs123 build_pairsIDX.py emo@104.197.126.67:~/             # Slave1
scp -i ~/.ssh/google-cloud-cs123 catalog.pkl emo@104.197.126.67:~/              # Slave1
scp -i ~/.ssh/google-cloud-cs123 run_mr_jobs_DATAPROC.sh emo@104.197.126.67:~/  # Slave1
scp -i ~/.ssh/google-cloud-cs123 pickle2distance1D_mr.py emo@104.197.126.67:~/  # Slave1


# Slave1:  104.197.126.67
# Master:  104.198.254.24
