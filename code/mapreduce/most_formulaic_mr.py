from mrjob.job import MRJob
from queue import PriorityQueue


class MostFormulaic(MRJob):
    '''
    This is based on taking distances between songs with distance correlation,
    which should be bounded between [0,1], with values closer to 0 indicating
    greater independence and values closer to 1 indicating less independence.
    '''

    def mapper(self, _, line):
        '''
        '''
        songIDX, distro_str = line.split('\t')

        distro_str = distro_str.strip('[]').split(',')

        mean_dist = float(distro_str[3])

        yield int(songIDX), mean_dist


    def reducer_init(self):
        self.songqueue = PriorityQueue(10)
        self.distances = []


    def reducer(self, songIDX, mean_dist):
        '''
        '''

        dist = list(mean_dist)[0]

        if len(self.distances) < 10:
            self.distances.append(abs(dist))
            self.songqueue.put(tuple((dist,songIDX)))
        elif abs(dist) > min(self.distances) and len(self.distances) == 10:
            self.distances.remove(min(self.distances))
            self.distances.append(abs(dist))
            _ = self.songqueue.get()
            self.songqueue.put(tuple((abs(dist),songIDX)))


    def reducer_final(self):
        while not self.songqueue.empty():
            distance, songIDX = self.songqueue.get()
            yield songIDX, distance


if __name__ == '__main__':
    MostFormulaic.run()
