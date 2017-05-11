from mrjob.job import MRJob


# This script yields TWO pairs of dissimilar songs.  Not sure what the problem
# is.

class MostSimilar(MRJob):
    '''
    This is based on taking distances between songs with distance correlation,
    which should be bounded between [0,1], with values closer to 0 indicating
    greater independence and values closer to 1 indicating less independence.
    '''

    def mapper(self, _, line):
        '''
        '''

        pair, dist = line.split('\t')

        yield pair, float(dist)


    def reducer_init(self):
        self.pairIDX = None
        self.distance = 0.0


    def reducer(self, pair, dist):
        '''
        '''

        distance = list(dist)[0]

        if distance > self.distance:
            self.distance = distance
            self.pairIDX = pair


    def reducer_final(self):
        pairIDX = self.pairIDX.strip('[]').split(',')
        pairIDX = [int(x) for x in pairIDX]
        yield pairIDX, self.distance


if __name__ == '__main__':
    MostSimilar.run()
