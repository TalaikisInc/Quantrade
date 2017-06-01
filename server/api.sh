#!/bin/bash

PROJECT=quantrade

cd /home/$PROJECT

source /usr/local/anaconda/bin/activate /usr/local/anaconda/envs/$PROJECT && \
  /usr/local/anaconda/envs/$PROJECT/bin/python /home/$PROJECT/run.py && \
  source /usr/local/anaconda/bin/deactivate
