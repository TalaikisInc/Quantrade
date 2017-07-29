#!/bin/bash

export LD_LIBRARY_PATH=/usr/local/anaconda/lib:$LD_LIBRARY_PATH

PROJECT=quantrade

cd /home/$PROJECT
source /usr/local/anaconda/bin/activate $PROJECT
/usr/local/anaconda/envs/$PROJECT/bin/python utils.py --mc=y
source /usr/local/anaconda/bin/deactivate
