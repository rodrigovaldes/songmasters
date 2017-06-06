import tables
import pickle
import numpy as np
from mpi4py import MPI
from queue import Queue
from scipy.misc import comb
import multiprocessing as mp
from operator import itemgetter
from itertools import combinations as combo
from sklearn.metrics.pairwise import cosine_similarity as cs
