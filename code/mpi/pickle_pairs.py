import pickle
from mpi4py import MPI
from queue import Queue
from scipy.misc import comb
from operator import itemgetter
from itertools import combinations as combo
from sklearn.metrics.pairwise import cosine_similarity as cs
import os

#Change as needed
NUM_PKLS = 10

NUM_PAIRS = int(comb(NUM_PKLS,2)) + NUM_PKLS

OUTPUT_DIR = 'distances'

M = 'pickles/music'
P = '.pkl'
D = '/distances/dist'
T = '.tsv'


def pick_pairs():
    '''
    '''
    
    q = Queue(NUM_PAIRS)
    print("q successful")

    for i, j in combo(range(0,NUM_PKLS),2):
        iPkl = M + str(i) + P
        jPkl = M + str(j) + P
        print("about to put something in the q")
        q.put(tuple((iPkl,jPkl)))

    for i in range(0,NUM_PKLS):
        q.put(tuple((M + str(i) + P, None)))
    
    print("end of pick_pairs()")
    return q


def process_pair(pair):
    '''
    '''
    a,b  = pair

    distances = []

    if b:
        for idxA, songA in a.items():
            for idxB, songB in b.items():
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
                songA = a[idxA]
                songB = b[idxB]
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
    fname = D + str(n) + T
    f = open(fname,'wb')
    for dist_tuple in distances:
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

    n = 0

    done = False

    while not done:

        if rank == 0:
            while not q.empty():
                print("start infinite loop")

                for i in range(1, size):
                        a,b = q.get()
                        pickleA = open(a,"rb")
                        print("success with P-A")

                        if b:
                            pickleB = open(b,"rb")
                            print("success P-B, in if B")
                        else:
                            pickleB = None
                            print("P-B == None")
                        pair = tuple((pickleA.read(), pickleB.read()))
                        print("success creation of pair")
                        
                        comm.send(pair,dest=i)
                        print('Sending pair to slave', i)

                print("Finish the sending loop")

                results = comm.gather()
                print('Received some results')
                write_dist(results,n)
                n += 1

            done = True

        else:
            done = True
            pair = tuple()
            print("First time not in the master node")

            #This may hang here once the master stops sending (but maybe not);
            #we could try this complicated tag thing to break out of the waiting to recv,
            #but there must be a better way
            #https://stackoverflow.com/questions/21088420/mpi4py-send-recv-with-tag
            # ^^ complicated tag thing

            if comm.recv(pair,source=0):    #Not sure if this will work
            #pair = comm.recv(source=0)
                print('\nSlave',rank,'received a pair')
                distances = process_pair(pair)
                comm.send(distances)
                print('\nSlave',rank,'sent distances')
                done = False

    return True

if __name__ == '__main__':


    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()
    print("first test")
    if rank == 0:
        create_output_dir()
        print("About to create q")
        q = pick_pairs()
    else:
        q = None
    print("out of first if else")

    process_pickle_pairs(q, rank, size)
