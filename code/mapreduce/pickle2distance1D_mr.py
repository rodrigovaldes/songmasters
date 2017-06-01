import pickle
import tables
import numpy as np
from mrjob.job import MRJob
#from scipy.spatial.distance import correlation as dc
from sklearn.metrics.pairwise import cosine_similarity as cs

from operator import itemgetter

DICTO = pickle.load(open('catalog.pkl', 'rb' ))


class SongDist(MRJob):
    '''
    '''

    def mapper(self, _, line):

        fields = line.strip('[]').split(',')
        idxA, idxB = int(fields[0]), int(fields[1])
        songA = DICTO[idxA]['features']
        songB = DICTO[idxB]['features']
        idx_pair = [idxA,idxB]
        dist = self.pairwise_comparison(songA, songB)

        yield idx_pair, dist




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

        songA = list(itemgetter('segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songA))
        songB = list(itemgetter('segLoudMax', 'segLoudMaxTime', 'segPitches', 'segStart', 'segTimbre')(songB))

        dist = self.distance(songA, songB)

        return dist
        # return "one"


    def flat(self, array_list):
        '''
        '''

        array_flat = array_list
        for i in range(len(array_flat)):
            array_flat[i] = array_flat[i].flatten()

        vector = np.concatenate(array_flat)

        return vector.reshape(1,-1)


    def distance(self, songA, songB):
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



if __name__ == '__main__':
    SongDist.run()
