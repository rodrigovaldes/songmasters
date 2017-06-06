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
NUM_PKLS = 12

NUM_PAIRS = int(comb(NUM_PKLS,2)) + NUM_PKLS

OUTFILE = 'all_distances.tsv'
OUTPUT_DIR = 'distances'

#PATH = '/mnt/storage/millon-song-dataset'
PATH = '/home/rvocss/song_data/MillionSongSubset/data'
PATH_SAVE = '/home/emo/distances'


#M = PATH + '/pickles/music'
#M = 'music'
#M = '/home/rvocss/songmasters/code/mpi/pickles/music'

M = 'pickles/music'

P = '.pkl'
D = 'distances/dist'
T = '.tsv'


def create_output_dir():
    '''
    Creates distances directory if it does not already exist.
    Inputs:
        None.
    Outputs:
        The output directory at current_path/OUTPUT_DIR.
    Returns:
        None.
    '''

    cur_path = os.path.split(os.path.abspath(__file__))[0]
    list_path = cur_path.split("/")
    get_first_elements = list_path[:3]
    new_dir = "/".join(get_first_elements) + "/"
    output_path = os.path.join(new_dir, OUTPUT_DIR)
    if not os.access(output_path, os.F_OK):
        os.makedirs(output_path)
        print('Created output directory:\n', output_path)


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

def resize_list_to_send(list_to_send, num_slaves):

    if len(list_to_send) == num_slaves:

        rv = list_to_send

    elif len(list_to_send) > num_slaves:

        ideal_by_node = int(round(len(list_to_send) / num_slaves, 0))

        len_last_bucket = len(list_to_send) - (ideal_by_node * (num_slaves - 1))

        index = 0
        new_list = []

        for i in range(num_slaves):

            if i < num_slaves - 1 :

                appendable_element = list_to_send[index: index + ideal_by_node]

            elif i == num_slaves - 1:

                appendable_element = list_to_send[-len_last_bucket:]

            new_list.append(appendable_element)

        rv = new_list

    return new_list


def write_dist_tsv(distances):

    first_step = [item for sublist in distances for item in sublist]

    for n, list_distances in enumerate(first_step):

        fname = D + str(n) + T
        f = open(fname,'w')

        for dist_tuple in list_distances:
            if dist_tuple:
                idxList, dist = dist_tuple
                if idxList[0] != idxList[1]:
                    big_string = str(idxList) + '\t' + str(dist)
                    f.write("%s\n" %  big_string)

        f.close()


def process_pickle_pairs(q, rank, size):
    '''
    '''

    i = 0

    while not q.empty():
        batch = []
        for i in range(size):
            if rank == 0:
                if not q.empty():
                    a,b = q.get()
                    print('Opening pickles')
                    pickleA = open(a,'rb').read()
                    #pickleA = pickle.loads(open(a,"rb"))
                    if b:
                        pickleB = open(b,'rb').read()
                        #pickleB = pickle.loads(open(b,"rb"))
                    else:
                        pickleB = None
                    pair = {'a':pickleA, 'b':pickleB}

                    batch.append(pair)

        pair = comm.scatter(batch, root=0)
        dist = process_pair(pair)
        distances = comm.gather(dist, root=0)

        if rank == 0:
            write_dist(distances,i)

        i += 1

def write_dist(distances,i):
    '''
    '''
    print('Writing to all_distances.tsv')
    f = open(OUTFILE,'a')
    if distances:
        for dist_list in distances:
            print('In outer for-loop')
            for dist_tuple in  dist_list:
                print('\tIn inner for-loop')
                if dist_tuple:
                    print('\t\tAbout to write')
                    idxList, dist = dist_tuple
                    big_string = str(idxList) + '\t' + str(dist)
                    f.write('%s\n' %  big_string)
                    print('\t\tJust wrote')
    else:
        print('distances is not valid')
    print('ABOUT TO CLOSE')
    f.close()
    print('CLOSED', i)

if __name__ == '__main__':

    create_output_dir()

    output = open(OUTFILE, 'w')
    output.close()

    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()

    print('Rank = {}; size = {}'.format(rank,size))

    q = pick_pairs()

    process_pickle_pairs(q, rank, size)
