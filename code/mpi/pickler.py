import os
import glob
import tables
import pickle
import numpy as np
import multiprocessing as mp

#Adjust as needed
MAX_SONGS = 100
NUM_PKLS = 2

SONGS_PER_PKL = int(MAX_SONGS / NUM_PKLS)

OUTPUT_DIR = 'pickles'

#Change this once the remote path is known
PATH = '/Users/erin/Desktop/MillionSongDataset/MillionSongSubset/data/'

def build_filelist():
    '''
    '''
    fname_list = []

    for i in range(MAX_SONGS):
        for fname in glob.glob(PATH + '**/*.h5',recursive=True):
            fname_list.append(tuple((i,fname)))
            i += 1

    return fname_list


def build_song(fname_tuple):
    '''
    Code for getters adapted from:
    https://raw.githubusercontent.com/tbertinmahieux/MSongsDB/master/PythonSrc/hdf5_getters.py
    '''

    songIDX, fname = fname_tuple

    #song = []
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

    # labels['features'] = []

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


    # for value in features.values():
    #     labels['features'].append(value)

    #labels['features'] = features

    '''
    return song
    '''

    return tuple((songIDX, labels, features))


#Need to make it stream through the song files, pickling as it goes
def pickle_pickles(fname_list):
    '''
    '''
    pickled_so_far, local_min = 0, 0

    next_thresh = SONGS_PER_PKL

    for i in range(NUM_PKLS):
        adminCat, musicalCat = {}, {}
        print('\ni = ',i)
        print('local_min = ', local_min)
        print('next_thresh = ', next_thresh)
        for fname_tuple in fname_list[local_min:next_thresh]:
            songIDX, adminDicto, musicalDicto = build_song(fname_tuple)
            print(songIDX)
            adminCat[songIDX] = adminDicto
            musicalCat[songIDX] = musicalDicto
            print('\t len(adminCat) =', len(adminCat.items()))
        local_min = next_thresh
        next_thresh += SONGS_PER_PKL
        pickle.dump(adminCat, open(OUTPUT_DIR + '/admin'+str(i)+'.pkl', 'wb'))
        pickle.dump(musicalCat, open(OUTPUT_DIR + '/music'+str(i)+'.pkl', 'wb'))


def create_output_dir():
    '''
    Creates directory if pickles directory does not already exist.
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
        print('Created output directory:', output_path)


if __name__ == '__main__':

    create_output_dir()

    print('Building filelist')
    fname_list = build_filelist()

    print('Built filelist, len = ', len(fname_list))
    print('Making pickles')

    pickle_pickles(fname_list)

    print('Pickles pickled')
