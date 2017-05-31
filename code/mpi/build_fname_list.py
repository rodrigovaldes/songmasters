import glob
import pickle

PATH = '/Users/erin/Desktop/MillionSongDataset/MillionSongSubset/data/'

def build_filelist():
    '''
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
