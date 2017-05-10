import tables
import numpy as np
from mrjob.job import MRJob
from scipy.spatial.distance import correlation as dc

#Need to change path once it is known for the remote machine
PATH = '/Users/erin/Desktop/MillionSongDataset/MillionSongSubset/data/'


class SongDist(MRJob):
    '''
    '''

    def mapper(self, _, line):
        fields = line.split(',')
        idxA, idxB = fields[0], fields[2]
        lpathA, lpathB = fields[1], fields[3]

        idx_pair = [int(idxA),int(idxB)]

        songA = self.build_song(lpathA)
        songB = self.build_song(lpathB)

        dist = self.pairwise_comparison(songA, songB)

        yield idx_pair, dist


    def build_song(self, lpath):
        '''
        Code for getters adapted from:
        https://raw.githubusercontent.com/tbertinmahieux/MSongsDB/master/PythonSrc/hdf5_getters.py
        '''

        fname = PATH + lpath

        song = []
        d = {}

        obj = tables.open_file(fname, mode='r')

        # Musical characteristics, 0-10
        d['sampleRate'] = np.array(obj.root.analysis.songs.cols.analysis_sample_rate[0])
        d['length'] = np.array(obj.root.analysis.songs.cols.duration[0])
        d['key'] = np.array(obj.root.analysis.songs.cols.key[0])
        d['loud'] = np.array(obj.root.analysis.songs.cols.loudness[0])
        d['tempo'] = np.array(obj.root.analysis.songs.cols.tempo[0])
        d['timeSignature'] = np.array(obj.root.analysis.songs.cols.time_signature[0])
        d['segLoudMax'] = obj.root.analysis.segments_loudness_max[obj.root.analysis.songs.cols.idx_segments_loudness_max[0]:]
        d['segLoudMaxTime'] = obj.root.analysis.segments_loudness_max_time[obj.root.analysis.songs.cols.idx_segments_loudness_max_time[0]:]
        d['segPitches'] = obj.root.analysis.segments_pitches[obj.root.analysis.songs.cols.idx_segments_pitches[0]:]
        d['segStart'] = obj.root.analysis.segments_start[obj.root.analysis.songs.cols.idx_segments_start[0]:]
        d['segTimbre'] = obj.root.analysis.segments_timbre[obj.root.analysis.songs.cols.idx_segments_timbre[0]:]

        obj.close()

        for key, value in d.items():
            song.append(value)

        return song


    def pad_array(self, array,newlen):
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


    def pairwise_comparison(self, songA, songB):
        '''
        '''

        songA_segs = len(songA[-1])
        songB_segs = len(songB[-1])

        if songA_segs != songB_segs:
            max_len = max(songA_segs, songB_segs)
            if max_len == songA_segs:
                #Pad arrays for songB
                for i in range(-5,0):
                    songB[i] = self.pad_array(songB[i],max_len)
            else:
                #Pad arrays for songA
                for i in range(-5,0):
                    songA[i] = self.pad_array(songA[i],max_len)

        dist = self.distance(songA[6:],songB[6:])

        return dist

    def flat(self, array_list):
        '''
        '''

        array_flat = array_list
        for i in range(len(array_flat)):
            array_flat[i] = array_flat[i].flatten()

        vector = np.concatenate(array_flat)

        return vector


    def distance(self, songA, songB):
        '''
        '''

        try:
            songA_vec = self.flat(songA)
            songB_vec = self.flat(songB)

            dist = dc(songA_vec,songB_vec)

            return dist

        except:
            print('Couldn\'t take distance for some reason')


if __name__ == '__main__':
    SongDist.run()
