#!/bin/bash

export LD_LIBRARY_PATH=/usr/local/anaconda/lib:$LD_LIBRARY_PATH


JOB_TYPE=$1

source /usr/local/anaconda/bin/activate quantrade && cd /home/quantrade && \
  /usr/local/anaconda/envs/quantrade/bin/python /home/quantrade/utils.py --$JOB_TYPE=True >> /home/quantrade/logs/$JOB_TYPE.log

source /usr/local/anaconda/bin/deactivate
