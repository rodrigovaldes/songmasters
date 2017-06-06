from scipy.misc import comb

NODES = 6

def diagnostic(pickles, NODES):
    com = comb(pickles,2)
    if (com + pickles) % NODES == 0:
        return True
    else:
        return False



def pick_pickles(lower, upper, NODES):
    for i in range(lower, upper + 1):
        if diagnostic(i, NODES):
            print(i)
