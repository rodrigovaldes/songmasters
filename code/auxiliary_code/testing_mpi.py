import os
import pickle
import numpy as np
from mpi4py import MPI
from queue import Queue
from scipy.misc import comb
from operator import itemgetter
from itertools import combinations as combo
from sklearn.metrics.pairwise import cosine_similarity as cs

comm = MPI.COMM_WORLD
rank, size = comm.Get_rank(), comm.Get_size()

print('Rank = {}; size = {}'.format(rank,size))

if rank == 0:

	create_mode = np.arange(size)

else:

    create_mode = None

one_part = comm.scatter(create_mode, root=0) 

one_part += 3

all_this = comm.gather(one_part, root=0)

if rank == 0:
	print(all_this)


# mpiexec -f hosts -n 2 python ~/songmasters/code/auxiliary_code/testing_mpi.py