from scipy.misc import comb


NODES = 6

UPPER = 100
LOWER = NODES

def diagnostic(pickles, NODES):
    com = comb(pickles,2)
    if (com + pickles) % NODES == 0:
        return True
    else:
        return False



def pick_pickles(LOWER, UPPER, NODES):
    print('The following pickle numbers are appropriate for use with {} nodes:'.format(NODES))
    for i in range(LOWER, UPPER + 1):
        if diagnostic(i, NODES):
            print(i)


if __name__ == '__main__':
    pick_pickles(LOWER,UPPER,NODES)
