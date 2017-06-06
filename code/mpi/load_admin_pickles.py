import pickle


'''
pickle2distance1D_mr.py:
distance correlation:  1m21.865s
cosine similarity:  1m58.006s
'''




catalog = pickle.load(open('catalog.pkl', "rb" ))



maxpickles = 69

pickles = []

for i in range(maxpickles):
    dicto = pickle.load(open('pickles/admin' + str(i) + '.pkl','rb'))
    pickles.append(dicto)
