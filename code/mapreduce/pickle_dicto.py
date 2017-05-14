import glob
import tables
import pickle
import numpy as np
import multiprocessing as mp


#Change this once the remote path is known
PATH = '/Users/erin/Desktop/MillionSongDataset/MillionSongSubset/data/'
# PATH = '/Users/ruy/Documents/UChicago/spring2017/cs123/song_data/MillionSongSubset/data/'


def build_filelist():
    '''
    '''
    fname_list = []

    i = 0

    for fname in glob.glob(PATH + '**/*.h5',
                           recursive=True):
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

    labels['features'] = features

    '''
    return song
    '''

    return tuple((songIDX, labels))


def build_catalog(songlist):
    '''
    '''

    dicto = {}

    for song in songlist:
        songIDX, songDicto = song
        dicto[songIDX] = songDicto

    return dicto


if __name__ == '__main__':

    fname_list = build_filelist()

    print('Built filelist')

    pool = mp.Pool(8)

    print('Initialized pool; building song dictionary')

    songlist = pool.map(build_song, fname_list)

    print('Built song list; building song catalog')

    catalog = build_catalog(songlist)

    print('Built catalog; pickling catalog')

    pickle.dump(catalog, open('catalog.pkl', 'wb'))

    print('Pickled catalog:  catalog.pkl')
