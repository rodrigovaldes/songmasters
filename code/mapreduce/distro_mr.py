import numpy as np
from mrjob.job import MRJob
from scipy.stats import skew as skew



class Distribution(MRJob):
    '''
    '''

    def mapper(self, _, line):
        '''
        '''
        pair, dist = line.split('\t')

        pair = pair.strip('[]').split(',')

        for song in pair:
            yield int(song), float(dist)




    def reducer(self, songIDX, dist):
        '''
        '''
        dicto = {}

        distro = np.array([x for x in dist])
        dicto['dMin'] = distro.min()
        dicto['dMAx'] = distro.max()
        dicto['dMedian'] = np.median(distro)
        dicto['dMean'] = distro.mean()
        dicto['dSD'] = distro.std()
        dicto['dSkew'] = skew(distro)

        value_list = []

        for value in dicto.values():
            value_list.append(value)

        yield songIDX, value_list



if __name__ == '__main__':
    Distribution.run()
