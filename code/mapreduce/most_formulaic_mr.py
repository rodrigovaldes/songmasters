from mrjob.job import MRJob
from queue import PriorityQueue


class MostFormulaic(MRJob):
    '''
    This is based on taking distances between songs with cosine similarity.
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
            self.distances.append(dist)
            self.songqueue.put(tuple((dist,songIDX)))
        elif dist > min(self.distances) and len(self.distances) == 10:
            self.distances.remove(min(self.distances))
            self.distances.append(dist)
            _ = self.songqueue.get()
            self.songqueue.put(tuple((dist,songIDX)))


    def reducer_final(self):
        while not self.songqueue.empty():
            distance, songIDX = self.songqueue.get()
            yield songIDX, distance


if __name__ == '__main__':
    MostFormulaic.run()
