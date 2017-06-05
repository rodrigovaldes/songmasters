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


def process_pickle_pairs(send_names_files, rank, size):
    '''
    '''
    print("arriving to pickle pair")

    if rank == 0:
        # list_to_send = resize_list_to_send(send_names_files, size) # DEL THIS
        list_to_send = np.arange(2)+ 1 
        # print("inside rank 0")
        # list_pickles = []
        # print(send_names_files[0])
        # for element in send_names_files:
        #     pickle_to_list_1 = pickle.load(open(element[0],"rb"))
        #     pickle_to_list_2 = pickle.load(open(element[1],"rb"))
        #     print("Creation of pkl success")
        #     list_pickles.append((pickle_to_list_1, pickle_to_list_2))
            # print(len(list_pickles))
            # print(len(list_pickles[0]))
            # # print(list_pickles[0])
            # print(list_pickles[0][0].keys())
            # print(list_pickles[0][0][0].keys())
    else:
        # list_pickles = None
        list_to_send = None
    
    print("out if else")
    print("This is list to send", list_to_send)
    print("The len of list to send is", len(list_to_send))
    # list_pickles = comm.scatter(list_pickles, root=0)
    list_to_send = comm.scatter(list_to_send, root=0) ## DEL THIS
    
     
    # print("before loop pairs")
    # for pair in list_pickles:
    #     print("about gen distmces")
    #     print(len(pair))
    #     distances = process_pair(pair)

    # for del_element in list_to_send: ### WHY it does not work?    ## DEL THIS
        # new_del = len(del_element)  ## DEL THIS

    print("Ir arrived the list_send", list_to_send)
    new_del = 1234 

    # all_distances = comm.gather(distances, root=0)

    all_distances = comm.gather(new_del, root=0) ## DEL THIS


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
        send_names_files = combinations_pickles()
        
        list_pickles = []
        for element in send_names_files:
            pickle_to_list_1 = pickle.load(open(element[0],"rb"))
            pickle_to_list_2 = pickle.load(open(element[1],"rb"))
            list_pickles.append((pickle_to_list_1, pickle_to_list_2))
        list_to_send = resize_list_to_send(list_pickles, size) # DEL THIS

    else:
        list_to_send = None

    list_to_send = comm.scatter(list_to_send, root=0) ## DEL THIS

    new_del = 1234 

    # process_pickle_pairs(send_names_files, rank, size)

    all_distances = comm.gather(new_del, root=0) ## DEL THIS

    if rank == 0:
        print(all_distances)





