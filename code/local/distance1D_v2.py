import glob
import tables
import numpy as np
import multiprocessing as mp
from itertools import combinations as combo
from scipy.spatial.distance import correlation as dc


PATH = '/Users/erin/Desktop/MillionSongDataset/MillionSongSubset/data/'


def build_filelist():
    '''
    '''
    fname_list = []

    i = 0

    for fname in glob.glob(PATH + '**/*.h5',recursive=True):
        fname_list.append(tuple((i,fname)))
        i += 1

    return fname_list


def build_song(fname_tuple):
    '''
    Code for getters adapted from:
    https://raw.githubusercontent.com/tbertinmahieux/MSongsDB/master/PythonSrc/hdf5_getters.py
    '''

    i, fname = fname_tuple

    song = []
    d = {}

    obj = tables.open_file(fname, mode='r')

    # Labels, 0-5
    d['i'] = i
    d['aName'] = str(obj.root.metadata.songs.cols.artist_name[0])[2:-1]
    d['aID'] = str(obj.root.metadata.songs.cols.artist_id[0])[2:-1]
    d['sName'] = str(obj.root.metadata.songs.cols.title[0])[2:-1]
    d['sID'] = str(obj.root.metadata.songs.cols.song_id[0])[2:-1]

    terms = obj.root.metadata.artist_terms[obj.root.metadata.songs.cols.idx_artist_terms[0]:]

    d['aTerms'] = []

    for term in terms:
        clean_term = str(term)[2:-1]
        d['aTerms'].append(clean_term)

    # Musical characteristics, 6-16
    d['sampleRate'] = np.array(obj.root.analysis.songs.cols.analysis_sample_rate[0])
    #d['dance'] = np.array([obj.root.analysis.songs.cols.danceability[0],0])
    d['length'] = np.array(obj.root.analysis.songs.cols.duration[0])
    d['key'] = np.array(obj.root.analysis.songs.cols.key[0])
    d['loud'] = np.array(obj.root.analysis.songs.cols.loudness[0])
    d['tempo'] = np.array(obj.root.analysis.songs.cols.tempo[0])
    d['timeSignature'] = np.array(obj.root.analysis.songs.cols.time_signature[0])
    #d['barsStart'] = obj.root.analysis.bars_start[obj.root.analysis.songs.cols.idx_bars_start[0]:]
    #d['beatsStart'] = obj.root.analysis.beats_start[obj.root.analysis.songs.cols.idx_beats_start[0]:]
    #d['sectionsStart'] = obj.root.analysis.sections_start[obj.root.analysis.songs.cols.idx_sections_start[0]:]
    d['segLoudMax'] = obj.root.analysis.segments_loudness_max[obj.root.analysis.songs.cols.idx_segments_loudness_max[0]:]
    d['segLoudMaxTime'] = obj.root.analysis.segments_loudness_max_time[obj.root.analysis.songs.cols.idx_segments_loudness_max_time[0]:]
    d['segPitches'] = obj.root.analysis.segments_pitches[obj.root.analysis.songs.cols.idx_segments_pitches[0]:]
    d['segStart'] = obj.root.analysis.segments_start[obj.root.analysis.songs.cols.idx_segments_start[0]:]
    d['segTimbre'] = obj.root.analysis.segments_timbre[obj.root.analysis.songs.cols.idx_segments_timbre[0]:]
    #d['tatumsStart'] = obj.root.analysis.tatums_start[obj.root.analysis.songs.cols.idx_tatums_start[0]:]

    obj.close()

    for key, value in d.items():
        song.append(value)

    return song


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


def pairwise_comparison(songpair):
    '''
    '''

    songA, songB = songpair

    songA_segs = len(songA[-1])
    songB_segs = len(songB[-1])

    if songA_segs != songB_segs:
        max_len = max(songA_segs, songB_segs)
        if max_len == songA_segs:
            #Pad arrays for songB
            for i in range(-5,0):
                songB[i] = pad_array(songB[i],max_len)
        else:
            #Pad arrays for songA
            for i in range(-5,0):
                songA[i] = pad_array(songA[i],max_len)


    dist = distance(songA[6:],songB[6:])

    elem = tuple((songA[0],songB[0],dist))

    return elem


def flat(array_list):
    '''
    '''

    array_flat = array_list
    for i in range(len(array_flat)):
        array_flat[i] = array_flat[i].flatten()

    vector = np.concatenate(array_flat)

    return vector


def distance(songA, songB):
    '''
    '''

    try:
        songA_vec = flat(songA)
        songB_vec = flat(songB)

        dist = dc(songA_vec,songB_vec)

        return dist

    except:
        print('Couldn\'t take distance for some reason')


def gen_pairs(songlist):
    '''
    '''

    pairs = combo(songlist,2)

    for pair in pairs:
        yield pair
