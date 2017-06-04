import os
import pickle
import numpy as np
from mpi4py import MPI
from queue import Queue
from scipy.misc import comb
from operator import itemgetter
from itertools import combinations as combo
from sklearn.metrics.pairwise import cosine_similarity as cs


#Change as needed
NUM_PKLS = 2#10

NUM_PAIRS = int(comb(NUM_PKLS,2)) + NUM_PKLS

OUTPUT_DIR = 'distances'

#PATH = '/mnt/storage/millon-song-dataset'
PATH = '/home/rvocss/song_data/MillionSongSubset/data'

#M = PATH + '/pickles/music'
#M = 'music'
M = '/home/rvocss/songmasters/code/mpi/pickles/music'

P = '.pkl'
D = 'distances/dist'
T = '.tsv'

def combinations_pickles():

    list_combinations = []

    for i,j in combo(range(0, NUM_PKLS), 2):
        iPkl = M + str(i) + P
        jPkl = M + str(j) + P
        list_combinations.append((iPkl, jPkl))

    for i in range(0, NUM_PKLS):
        list_combinations.append((M + str(i) + P, M + str(i) + P))

    return list_combinations


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
    a,b  = pair

    distances = []

    for idxA, songA in a.items():
        for idxB, songB in b.items():
            idxList = [idxA, idxB]
            dist = pairwise_comparison(songA,songB)
            distances.append(tuple((idxList,dist)))

    return distances


def process_pickle_pairs(send_names_files, rank, size):
    '''
    '''
    print("arriving to pickle pair")

    if rank == 0:
        print("inside rank 0")
        list_pickles = []
        for element in send_names_files:
            pickle_to_list_1 = pickle.load(open(element[0],"rb"))
            pickle_to_list_2 = pickle.load(open(element[1],"rb"))
            list_pickles.append((pickle_to_list_1, pickle_to_list_2))
    else:
        list_pickles = None

    list_pickles = comm.scatter(list_pickles, root=0)

    for pair in list_pickles:

        distances = process_pair(pair)

    all_distances = comm.gather(distances, root=0)

    if rank == 0:
        print(all_distances)


    # else:
    #     print("inside other rank non zero. Waiting for info")
    #     pair = comm.recv(source=0)
    #     print("information arrived")
    #     print("about to obtain distances")
    #     distances = process_pair(pair)
    #     print(distances)
    #     print("distances done")

    
    # results = comm.gather(distances, root=0)

    # if rank == 0:
    #     print("I'm in the writing part")
    #     write_dist(results,n)
    #     n += 1


if __name__ == '__main__':

    print('Entered main')
    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()

    print('Rank = {}; size = {}'.format(rank,size))

    if rank == 0:
        create_output_dir()
        print('Make list of name files to distribute')
        send_names_files = combinations_pickles()
        print("finish with the names_files_list")
    else:
        send_names_files = None

    print('About to call process_pickle_pairs')
    process_pickle_pairs(send_names_files, rank, size)

