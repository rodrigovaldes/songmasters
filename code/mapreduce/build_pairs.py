import glob
import multiprocessing as mp
from itertools import combinations as combo

#Need to change path once it is known on remote machine
PATH = '/Users/erin/Desktop/MillionSongDataset/MillionSongSubset/data/'


def build_filelist():
    '''
    '''
    fname_list = []

    i = 0

    for fname in glob.glob(PATH + '**/*.h5',recursive=True):
        local_path = fname[len(PATH):]
        fname_list.append(tuple((str(i),local_path)))
        i += 1

    return fname_list


def gen_pairs(fname_list):
    '''
    '''

    pairs = combo(fname_list,2)

    for pair in pairs:
        yield pair


def write_pairs(fname_pairs):
    '''
    '''

    fout = open('pairs.csv', 'w')

    for pair in fname_pairs:
        idxA, fnameA = pair[0]
        idxB, fnameB = pair[1]
        fout.write(idxA+ ',' + fnameA + ',' + idxB + ',' + fnameB + '\n')

    fout.close()


if __name__ == '__main__':

    fname_list = build_filelist()

    print('Built filelist')

    print('Building pairs of filenames')

    fname_pairs = gen_pairs(fname_list)

    print('Built fname_pairs')

    write_pairs(fname_pairs)

    print('Wrote pairs.csv')
