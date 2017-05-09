from distance1D_v2 import *



if __name__ == '__main__':

    fname_list = build_filelist()

    print('Built filelist')

    pool = mp.Pool(8)

    print('Initialized pool; building songlist')

    songlist = pool.map(build_song, fname_list[0:548])

    print('Built songlist')

    pair_list = gen_pairs(songlist)

    print('Built pair_list')

    dist_list = pool.map(pairwise_comparison,pair_list)

    print('Built distance list')
