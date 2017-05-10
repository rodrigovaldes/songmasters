import numpy as np
from mrjob.job import MRJob
from scipy.stats import skew as skew

#9552	[0.5286862605730831, 0.9920071518476695, 0.8119728761566468, 0.798583681936698, 0.09800582444148233, -0.31780970738749975]

class MetaDistribution(MRJob):
    '''
    '''

    def mapper(self, _, line):
        '''
        '''
        songIDX, distro_str = line.split('\t')

        distro_str = distro_str.strip('[]').split(',')

        dicto = {'dMin':None}
        dicto['dMAx'] = None
        dicto['dMedian'] = None
        dicto['dMean'] = None
        dicto['dSD'] = None
        dicto['dSkew'] = None

        i = 0

        for key in dicto.keys():
            value = float(distro_str[i])
            dicto[key] = value
            i += 1
            yield key, value




    def reducer(self, key, value):
        '''
        '''

        yield key, np.mean([x for x in value])




if __name__ == '__main__':
    MetaDistribution.run()
