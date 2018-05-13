#!/bin/bash

PROJECT=qprob

cd /home/$PROJECT/api_server

source /usr/local/anaconda/bin/activate /usr/local/anaconda/envs/$PROJECT && \
  /home/$PROJECT/api_server/api_server && \
  source /usr/local/anaconda/bin/deactivate

