from mrjob.job import MRJob


# This script yields TWO pairs of similar songs.  Not sure what the problem is.
#
# Seems to create two reducer_inits but I don't understand why.

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
        print('\n\nIn reducer_init')
        self.pairIDX = None
        self.distance = 0.0
        self.i = 0


    def reducer(self, pair, dist):
        '''
        '''

        distance = list(dist)[0]

        if distance > self.distance:
            print('\n\nIn reducer, inside if-statement')
            print('\tself.i = {}'.format(self.i))
            print('\tdistance = {}; self.distance = {}'.format(distance,self.distance))
            print('\tpair = {}; self.pairIDX = {}'.format(pair,self.pairIDX))
            self.distance = distance
            self.pairIDX = pair

        self.i += 1


    def reducer_final(self):
        print('\n\nIn reducer_final')
        pairIDX = self.pairIDX.strip('[]').split(',')
        pairIDX = [int(x) for x in pairIDX]
        yield pairIDX, self.distance


if __name__ == '__main__':
    MostSimilar.run()
