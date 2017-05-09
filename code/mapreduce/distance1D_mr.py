import glob
import tables
#import sklearn
import numpy as np
from mrjob.job import MRJob
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


def gen_pairs(fname_list):
    '''
    '''

    pairs = combo(fname_list,2)

    for pair in pairs:
        yield pair

def write_pairs(fname_pairs):
    '''
    '''

    fout = open('pairs.csv', 'w')

    for pair in fname_pairs:
        idxA, fnameA = pair[0]
        idxB, fnameB = pair[1]
        fout.write(str(idxA)+ ',' + fnameA + ',' + str(idxB) + ',' + fnameB + '\n')

    fout.close()

if __name__ == '__main__':

    '''
    fname_list = build_filelist()

    print('Built filelist')

    pool = mp.Pool(8)

    print('Initialized pool; building songlist')

    songlist = pool.map(build_song, fname_list)

    print('Built songlist')

    pair_list = list(combo(songlist,2))

    print('Built pair_list')

    dist_list = pool.map(pairwise_comparison,pair_list)

    print('Built distance list')
    '''


    #Pairwise comparisons to determine average distance from a given song to all other songs
    #Density of clustering
    #Pad to length with zeros only during pairwise comparisons


    #
    #Distance correlation:  just write the function ourselves (I think this can work with differently shaped data, but I'm not sure)
    #Distortion should be contained only to parts that need to be padded; beginnings of all corresponding values should line up
    #Lit Review?  How to compute distances between things that are different lengths; figure out which ideas apply and which don't
    #Manage time well.
    #Big data stuff is more important than methodological perfection, at least in this case.


    # What do we do with the distance data when we get it?
    # Density:  Look at a particular song:  the average distance between that song and other songs is low, then there are a lot of songs similar to it
    # Tells you about the density of songs:  some songs more similar; other songs more eclectic and distinct
    # If we look at one song and the distances to all the other songs, we can cluster based on a threshold of all the songs within a certain distance of the song
    # Within different genres, there are different subgenres and types of songs
        # Relative to a specific song

    # What if we chose two songs and used distance from them as axes

    # Trust the tags:  if you center the distance metric at a protototypical song in a given genre, how does the genre map change if you then center it on a different genre

    # Get Wikipedia data on artist genre as the definitive genre





    # What if we impute genres using modal tag among similar artists?
        #But what if there is a full network of artists with no isolates?
        #That doesn't use distances, though

    # What if we define similar artists using song distances?
        # Undirected graph of artist network where edges are distance between songs?
                # UNDIRECTED GRAPH OF SONGS WITH EDGES AS DISTANCES BETWEEN SONG NODES
                    # Does this just bring us back to the same issues we had before
                    # about not being able to get distance down to two dimensions?
                    # Would this just be a mess with 500 billion edges cluttering up the visualization?

    #Give each song a distribution of distances
    #min, max, mean, sd

    #Find most eclectic songs and most formulaic songs in the dataset

    #Find the most representative song in each genre

    '''
    In [71]: obj.root.metadata.songs.cols
    Out[71]:
    /metadata/songs.cols (Cols), 20 columns
      analyzer_version (Column(1,), |S32)
      artist_7digitalid (Column(1,), int32)
      artist_familiarity (Column(1,), float64)
      artist_hotttnesss (Column(1,), float64)
      artist_id (Column(1,), |S32)
      artist_latitude (Column(1,), float64)
      artist_location (Column(1,), |S1024)
      artist_longitude (Column(1,), float64)
      artist_mbid (Column(1,), |S40)
      artist_name (Column(1,), |S1024)
      artist_playmeid (Column(1,), int32)
      genre (Column(1,), |S1024)
      idx_artist_terms (Column(1,), int32)
      idx_similar_artists (Column(1,), int32)
      release (Column(1,), |S1024)
      release_7digitalid (Column(1,), int32)
      song_hotttnesss (Column(1,), float64)
      song_id (Column(1,), |S32)
      title (Column(1,), |S1024)
      track_7digitalid (Column(1,), int32)

    In [107]: obj.root.analysis.songs.cols
    Out[107]:
    /analysis/songs.cols (Cols), 31 columns
      analysis_sample_rate (Column(1,), int32)
      audio_md5 (Column(1,), |S32)
      danceability (Column(1,), float64)
      duration (Column(1,), float64)
      end_of_fade_in (Column(1,), float64)
      energy (Column(1,), float64)
      idx_bars_confidence (Column(1,), int32)
      idx_bars_start (Column(1,), int32)
      idx_beats_confidence (Column(1,), int32)
      idx_beats_start (Column(1,), int32)
      idx_sections_confidence (Column(1,), int32)
      idx_sections_start (Column(1,), int32)
      idx_segments_confidence (Column(1,), int32)
      idx_segments_loudness_max (Column(1,), int32)
      idx_segments_loudness_max_time (Column(1,), int32)
      idx_segments_loudness_start (Column(1,), int32)
      idx_segments_pitches (Column(1,), int32)
      idx_segments_start (Column(1,), int32)
      idx_segments_timbre (Column(1,), int32)
      idx_tatums_confidence (Column(1,), int32)
      idx_tatums_start (Column(1,), int32)
      key (Column(1,), int32)
      key_confidence (Column(1,), float64)
      loudness (Column(1,), float64)
      mode (Column(1,), int32)
      mode_confidence (Column(1,), float64)
      start_of_fade_out (Column(1,), float64)
      tempo (Column(1,), float64)
      time_signature (Column(1,), int32)
      time_signature_confidence (Column(1,), float64)
      track_id (Column(1,), |S32)
    '''
