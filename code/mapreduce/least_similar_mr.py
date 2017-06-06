from mrjob.job import MRJob


class LeastSimilar(MRJob):
    '''
    This is based on taking distances between songs with cosine similarity.
    '''

    def mapper(self, _, line):
        '''
        '''

        pair, dist = line.split('\t')

        yield pair, float(dist)


    def reducer_init(self):
        self.pairIDX = None
        self.distance = 1.0


    def reducer(self, pair, dist):
        '''
        '''

        distance = list(dist)[0]

        if distance < self.distance:
            self.distance = distance
            self.pairIDX = pair

    def reducer_final(self):
        pairIDX = self.pairIDX.strip('[]').split(',')
        pairIDX = [int(x) for x in pairIDX]
        yield pairIDX, self.distance


if __name__ == '__main__':
    LeastSimilar.run()
