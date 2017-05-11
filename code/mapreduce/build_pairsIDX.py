from itertools import combinations as combo


def gen_pairs(idx_list):
    '''
    '''

    pairs = combo(idx_list,2)

    for pair in pairs:
        yield pair


def write_pairs(pair_list):
    '''
    '''

    fout = open('pairsIDX.csv', 'w')

    for pair in pair_list:
        idxA, idxB = pair
        fout.write(str(idxA) + ',' + str(idxB) + '\n')

    fout.close()



if __name__ == '__main__':

    idx_list = [x for x in range(0,10000)]

    pair_list = gen_pairs(idx_list)

    write_pairs(pair_list)
