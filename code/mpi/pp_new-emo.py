import os
import pickle
import time, sys
import numpy as np
from mpi4py import MPI
from queue import Queue
from scipy.misc import comb
from operator import itemgetter
from itertools import combinations as combo
from sklearn.metrics.pairwise import cosine_similarity as cs


#Change as needed
NUM_PKLS = 8#15

NUM_NODES = 6

NUM_PAIRS = int(comb(NUM_PKLS,2)) + NUM_PKLS

OUTFILE = 'all_distances.tsv'

M = 'pickles/music'

P = '.pkl'

def pick_pairs():
    '''
    '''

    q = Queue(NUM_PAIRS)

    for i, j in combo(range(0,NUM_PKLS),2):
        iPkl = M + str(i) + P
        jPkl = M + str(j) + P
        q.put(tuple((iPkl,jPkl)))

    for i in range(0,NUM_PKLS):
        q.put(tuple((M + str(i) + P, None)))

    return q


def pad_array(array,newlen):
    '''
    '''
    currentlen = array.shape[0]
    delta = newlen - currentlen

    if len(array.shape) == 2:
        currentwidth = array.shape[1]
        blanks = np.zeros([delta,currentwidth])

    else:
        blanks = np.zeros(delta)

    try:
        new_array = np.concatenate([array,blanks])

        return new_array

    except:
        print('ERROR:')


def flat(array_list):
    '''
    '''

    array_flat = array_list
    for i in range(len(array_flat)):
        array_flat[i] = array_flat[i].flatten()

    vector = np.concatenate(array_flat)

    return vector.reshape(1,-1)


def distance(songA, songB):
    '''
    '''

    try:
        songA_vec = flat(songA)
        songB_vec = flat(songB)

        dist = cs(songA_vec,songB_vec)

        return dist[0][0]

    except:
        print('Couldn\'t take distance for some reason')


def pairwise_comparison(songA, songB):
    '''
    '''

    songA_segs = len(songA["segTimbre"])
    songB_segs = len(songB["segTimbre"])

    if songA_segs != songB_segs:
        max_len = max(songA_segs, songB_segs)
        if max_len == songA_segs:
            for i in ['segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre']:
                songB[i] = pad_array(songB[i],max_len)
        else:
            for i in ['segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre']:
                songA[i] = pad_array(songA[i],max_len)


    songA = list(itemgetter('sampleRate','length','key','loud','tempo','timeSignature','segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songA))
    songB = list(itemgetter('sampleRate','length','key','loud','tempo','timeSignature','segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songB))

    dist = distance(songA, songB)

    return dist

def process_pair(pair):
    '''
    '''
    a,b  = pair['a'], pair['b']

    unpickleA = pickle.loads(a)

    distances = []

    if b:
        unpickleB = pickle.loads(b)
        for idxA, songA in unpickleA.items():
            for idxB, songB in unpickleB.items():
                idxList = [idxA, idxB]
                dist = pairwise_comparison(songA,songB)
                distances.append(tuple((idxList,dist)))
    else:
        keys = list(unpickleA.keys())
        num_songs = len(keys)
        for i in range(num_songs):
            for j in range(i + 1, num_songs):
                idxA = keys[i]
                idxB = keys[j]
                songA = unpickleA[idxA]
                songB = unpickleA[idxB]
                idxList = [idxA,idxB]
                dist = pairwise_comparison(songA,songB)
                distances.append(tuple((idxList,dist)))

    return distances


def process_pickle_pairs(q, rank, size):
    '''
    '''

    while not q.empty():
        batch = []
        for i in range(size):
            if rank == 0:
                if not q.empty():
                    a,b = q.get()
                    pickleA = open(a,'rb').read()
                    if b:
                        pickleB = open(b,'rb').read()
                    else:
                        pickleB = None
                    pair = {'a':pickleA, 'b':pickleB}

                    batch.append(pair)

        pair = comm.scatter(batch, root=0)
        dist = process_pair(pair)
        distances = comm.gather(dist, root=0)

        if rank == 0:
            write_dist(distances)

def write_dist(distances):
    '''
    '''
    f = open(OUTFILE,'a')
    if distances:
        for dist_list in distances:
            for dist_tuple in  dist_list:
                if dist_tuple:
                    idxList, dist = dist_tuple
                    big_string = str(idxList) + '\t' + str(dist)
                    f.write('%s\n' %  big_string)
    else:
        print('distances is not valid')
    f.close()

if __name__ == '__main__':

    if (NUM_PAIRS % NUM_NODES) == 0:

        output = open(OUTFILE, 'w')
        output.close()

        comm = MPI.COMM_WORLD
        rank, size = comm.Get_rank(), comm.Get_size()

        print('Rank = {}; size = {}'.format(rank,size))

        q = pick_pairs()

        process_pickle_pairs(q, rank, size)

        print("I'm tired.  It's almost 4:00 a.m. and the birds are singing.  This should end...")
        time.sleep(1)
        print("... about now.")
        time.sleep(1)
        print("Yes, we are bad kids. We do not want to work anymore.")
        print("But don't worry. We finished the job!")
        time.sleep(1)
        print("Bye!")
        print('\n\nP.S.  This hangs and we don\'t know why.  Please hit ctrl-z to exit.')
        sys.exit(0)
