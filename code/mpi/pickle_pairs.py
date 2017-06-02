import pickle
from mpi4py import MPI
from queue import Queue
from scipy.misc import comb
from operator import itemgetter
from itertools import combinations as combo
from sklearn.metrics.pairwise import cosine_similarity as cs


#Change as needed
NUM_PKLS = 10
#NUM_SLAVES = 10

NUM_PAIRS = int(comb(NUM_PKLS,2)) + NUM_PKLS

OUTPUT_DIR = 'distances'

M = '/pickles/music'
P = '.pkl'
D = '/distances/dist'
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


def deliver_pickles(q):
    '''
    '''

    if rank == 0:
        if not q.empty():
            for i in range(1,size + 1):
                a,b = q.get()
                unPickleA = pickle.load(open(a, "rb" ))
                if b:
                    unPickleB = pickle.load(open(b,"rb"))
                else:
                    unPickleB = None
                pair = tuple((unPickleA, unPickleB))

                comm.Send(pair,dest=i)
                print('Sending pair to slave', i)

            return rank, q

        else:
            print('Sent all pairs for processing')
            return None, None
    else:
        pair = tuple()
        comm.recv(pair,source=0)
        print('Slave',rank,'received a pair')

        return rank, pair


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
                songB[i] = self.pad_array(songB[i],max_len)
        else:
            #Pad arrays for songA
            # THIS IS WRONG, NEED TO CHANGE DIC TO LIST from Pickle
            for i in ['segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre']:
                songA[i] = self.pad_array(songA[i],max_len)

    #songA = list(itemgetter('segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songA))
    #songB = list(itemgetter('segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songB))

    songA = list(itemgetter('sampleRate','length','key','loud','tempo','timeSignature','segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songA))
    songB = list(itemgetter('sampleRate','length','key','loud','tempo','timeSignature','segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songB))

    dist = self.distance(songA, songB)

    return dist
    # return "one"


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
        songA_vec = self.flat(songA)
        songB_vec = self.flat(songB)

        #dist = dc(songA_vec,songB_vec)
        dist = cs(songA_vec,songB_vec)

        return dist[0][0]

    except:
        print('Couldn\'t take distance for some reason')


def write_dist(distances):
    '''
    '''
    fname = D + str(RECV) + T
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


if __name__ == '__main__':

    create_output_dir()

    q = pick_pairs()

    RECV = 0

    comm = MPI.COMM_WORLD
    rank, size = comm.Get_rank(), comm.Get_size()

    rRank, obj = deliver_pickles(q) # What is rRank??
    SENT = size - 1

    if rRank:
        pair = obj
        # q = 0 # We can delete this
    else:
        q = obj ## This will break the code if it arrive to q.get() a few lines ahead
        pair = 0

    if pair:
        results = process_pair(pair) ### Process pair require a tuple. This will brake when pair == 0
        comm.send(results, dest=0)#, tag=rank)
    else:
        while SENT < NUM_PAIRS or RECV < NUM_PAIRS:
            #http://nullege.com/codes/search/mpi4py.MPI.ANY_SOURCE
            distances = comm.recv(source=MPI.ANY_SOURCE, status=status) ## Apparently, this may run for all the nodes and not only the master
            incoming_rank = source.Get_source()
            write_dist(distances)
            RECV += 1
            if SENT < NUM_PAIRS:
                pair = q.get()
                comm.send(pair, dest=incoming_rank)
                SENT += 1



            '''
            '''
