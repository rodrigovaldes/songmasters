#!/bin/sh

sudo apt-get install bzip2

wget https://repo.continuum.io/archive/Anaconda3-4.4.0-Linux-x86_64.sh

bash Anaconda3-4.4.0-Linux-x86_64.sh -b -p ~/anaconda3


exec bash

export PATH=~/anaconda3/bin:$PATH
conda install -c anaconda mpi4py=2.0.0 -y
