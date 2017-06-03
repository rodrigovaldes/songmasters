import os
import tables
import pickle
import numpy as np
import multiprocessing as mp

#Adjust as needed; MAX_SONGS % NUM_PKLS should equal 0
MAX_SONGS = 10000
NUM_PKLS = 10

SONGS_PER_PKL = int(MAX_SONGS / NUM_PKLS)

OUTPUT_DIR = 'pickles'

#Change this once the remote path is known
#PATH = '/Users/erin/Desktop/MillionSongDataset/MillionSongSubset/data/'
#PATH = '/mnt/storage/millon-song-dataset/'
PATH = 'home/rvocss/song_data/MillionSongSubset/data'

def build_song(fname_tuple):
    '''
    Pulls out important administrative and musical elements from a songfile and
    stores them in dictionaries.  Returns a tuple of the song index and the
    dictionaries.

    Code for getters adapted from:
    https://raw.githubusercontent.com/tbertinmahieux/MSongsDB/master/PythonSrc/hdf5_getters.py
    '''

    songIDX, fname = fname_tuple

    labels, features = {}, {}

    obj = tables.open_file(fname, mode='r')

    # Labels, 0-4
    labels['aName'] = str(obj.root.metadata.songs.cols.artist_name[0])[2:-1]
    labels['aID'] = str(obj.root.metadata.songs.cols.artist_id[0])[2:-1]
    labels['sName'] = str(obj.root.metadata.songs.cols.title[0])[2:-1]
    labels['sID'] = str(obj.root.metadata.songs.cols.song_id[0])[2:-1]

    terms = obj.root.metadata.artist_terms[obj.root.metadata.songs.cols.idx_artist_terms[0]:]

    labels['aTerms'] = []

    for term in terms:
        clean_term = str(term)[2:-1]
        labels['aTerms'].append(clean_term)

    # Musical characteristics, 5-15
    features['sampleRate'] = np.array(obj.root.analysis.songs.cols.analysis_sample_rate[0])
    features['length'] = np.array(obj.root.analysis.songs.cols.duration[0])
    features['key'] = np.array(obj.root.analysis.songs.cols.key[0])
    features['loud'] = np.array(obj.root.analysis.songs.cols.loudness[0])
    features['tempo'] = np.array(obj.root.analysis.songs.cols.tempo[0])
    features['timeSignature'] = np.array(obj.root.analysis.songs.cols.time_signature[0])
    features['segLoudMax'] = obj.root.analysis.segments_loudness_max[obj.root.analysis.songs.cols.idx_segments_loudness_max[0]:]
    features['segLoudMaxTime'] = obj.root.analysis.segments_loudness_max_time[obj.root.analysis.songs.cols.idx_segments_loudness_max_time[0]:]
    features['segPitches'] = obj.root.analysis.segments_pitches[obj.root.analysis.songs.cols.idx_segments_pitches[0]:]
    features['segStart'] = obj.root.analysis.segments_start[obj.root.analysis.songs.cols.idx_segments_start[0]:]
    features['segTimbre'] = obj.root.analysis.segments_timbre[obj.root.analysis.songs.cols.idx_segments_timbre[0]:]

    obj.close()

    return tuple((songIDX, labels, features))


def pickle_pickles(IDX_tuple):
    '''
    Pickles a batch of songs into two pickles, administrative and musical.
    '''
    i, local_min, next_thresh = IDX_tuple

    adminCat, musicalCat = {}, {}

    for fname_tuple in fname_list[local_min:next_thresh]:
        songIDX, adminDicto, musicalDicto = build_song(fname_tuple)
        adminCat[songIDX] = adminDicto
        musicalCat[songIDX] = musicalDicto
    #local_min = next_thresh
    #next_thresh += SONGS_PER_PKL
    pickle.dump(adminCat, open(OUTPUT_DIR + '/admin'+str(i)+'.pkl', 'wb'))
    pickle.dump(musicalCat, open(OUTPUT_DIR + '/music'+str(i)+'.pkl', 'wb'))


def create_output_dir():
    '''
    Creates pickles directory if it does not already exist.
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


def create_IDX_list():
    '''
    Creates a list of tuples of the following format: (i, start, end), where i
    is the pickle number, start is the starting song index and end is 1 + the
    ending song index.
    '''

    IDX_list = []

    for i in range(NUM_PKLS):
        start = i * SONGS_PER_PKL
        end = (i + 1) * SONGS_PER_PKL
        IDX_list.append(tuple((i,start,end)))

    return IDX_list


if __name__ == '__main__':

    create_output_dir()

    print('Loading filelist')
    fname_list = pickle.load(open('fname_list.pkl','rb'))
    print('Loaded filelist')

    print('Building IDX list')
    IDX_list = create_IDX_list()
    print('Built IDX list')

    print('Pickling pickles')
    #Update for production environment
    pool = mp.Pool(8)
    pool.map(pickle_pickles, IDX_list)
    pickle.dump(IDX_list, open('IDX_list.pkl', 'wb'))
    print('Pickles pickled')
