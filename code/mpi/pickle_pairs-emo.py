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


def process_pair(pair):
    '''
    '''
    a,b  = pair['a'], pair['b']

    # unpickleA = pickle.loads(a)
    unpickleA = a 
    distances = []

    if b:
        # unpickleB = pickle.loads(b)
        unpickleB = b
        for idxA, songA in unpickleA.items():
            for idxB, songB in unpickleB.items():
                idxList = [idxA, idxB]
                dist = pairwise_comparison(songA,songB)
                distances.append(tuple((idxList,dist)))
    else:
        keys = list(a.keys())
        num_songs = len(keys)
        for i in range(num_songs):
            for j in range(i + 1, num_songs):
                idxA = keys[i]
                idxB = keys[j]
                songA = unpickleA[idxA]
                songB = unpickleB[idxB]
                idxList = [idxA,idxB]
                dist = pairwise_comparison(songA,songB)
                distances.append(tuple((idxList,dist)))

    return distances

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
        print('newlen = {}; currentlen = {}; delta = {}'.format(newlen,currentlen,delta))
        print('array.shape =',array.shape)
        print('blanks.shape =',blanks.shape)


def pairwise_comparison(songA, songB):
    '''
    '''

    songA_segs = len(songA["segTimbre"])
    songB_segs = len(songB["segTimbre"])

    if songA_segs != songB_segs:
        max_len = max(songA_segs, songB_segs)
        if max_len == songA_segs:
            #Pad arrays for songB
            # THIS IS WRONG, NEED TO CHANGE DIC TO LIST from Pickle
            for i in ['segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre']:
                songB[i] = pad_array(songB[i],max_len)
        else:
            #Pad arrays for songA
            # THIS IS WRONG, NEED TO CHANGE DIC TO LIST from Pickle
            for i in ['segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre']:
                songA[i] = pad_array(songA[i],max_len)

    #songA = list(itemgetter('segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songA))
    #songB = list(itemgetter('segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songB))

    songA = list(itemgetter('sampleRate','length','key','loud','tempo','timeSignature','segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songA))
    songB = list(itemgetter('sampleRate','length','key','loud','tempo','timeSignature','segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songB))

    dist = distance(songA, songB)

    return dist


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


def write_dist(distances,n):
    '''
    '''
    print("distances=",distances)
    fname = D + str(n) + T
    f = open(fname,'wb')
    if distances:
        for dist_tuple in distances:
            if dist_tuple:
                idxList, dist = dist_tuple
                f.write(idxList,'\t',dist,'\n')
        f.close()


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
    output_path = os.path.join(cur_path, OUTPUT_DIR)
    if not os.access(output_path, os.F_OK):
        os.makedirs(output_path)
        print('Created output directory:\n', output_path)



def process_pickle_pairs(q, rank, size):
    '''
    '''
    print("arrived to process_pickle_pairs")
    n = 0
    if rank == 0:
        print("Beginning rank 0")
        while not q.empty():
            for i in range(1, size):
                    a,b = q.get()
                    print('Loading pickles')
                    #pickleA = open(a,'rb').read()
                    pickleA = pickle.load(open(a,"rb"))
                    if b:
                        #pickleB = open(b,'rb').read()
                        pickleB = pickle.load(open(b,"rb"))
                    else:
                        pickleB = None
                    pair = {'a':pickleA, 'b':pickleB}

                    comm.send(pair,dest=i)
                    print('Sending pair to slave', i)
        distances = []


    else:
        print("NOT node 1. About to wait pair")
        pair = comm.recv(source=0)
        print(pair["a"])
        print("received pair. About to cal didtamnces")
        distances = process_pair(pair)
        print(distances)
        print("finish with distances")
    
    print("about to gather")
    distances = []
    results = comm.gather(distances, root=0)
    print(results)
    print("after gather")

    if rank == 0:
        write_dist(results,n)
        n += 1


if __name__ == '__main__':

    print('Entered main')
    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()

    print('Rank = {}; size = {}'.format(rank,size))

    if rank == 0:
        create_output_dir()
        print('Making queue')
        q = pick_pairs()
        print("finish with the q")
    else:
        q = None

    print('About to call process_pickle_pairs')
    process_pickle_pairs(q, rank, size)
