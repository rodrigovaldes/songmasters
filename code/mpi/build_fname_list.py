import glob
import pickle

PATH = '/Users/erin/Desktop/MillionSongDataset/MillionSongSubset/data/'
#PATH = '/mnt/storage/millon-song-dataset/'
#PATH = '/home/rvocss/song_data/MillionSongSubset/data/'

def build_filelist():
    '''
    Builds a filelist of all the songfiles in the dataset.  Entries in the list
    are in the following format:  (i, fname), where i is the song's index in the
    list and fname is the song's filepath.
    '''

    fname_list = []

    i = 0

    for fname in glob.glob(PATH + '**/*.h5',recursive=True):
        fname_list.append(tuple((i,fname)))
        i += 1

    return fname_list


if __name__ == '__main__':

    print('Building filelist')
    fname_list = build_filelist()
    pickle.dump(fname_list, open('fname_list.pkl', 'wb'))
    print('Built & pickled filelist')
