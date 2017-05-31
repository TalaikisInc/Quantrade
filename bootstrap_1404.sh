#!/usr/bin/env bash

sudo apt update
sudo apt upgrade -y
cd ~/
sudo apt install gcc -y
git clone https://github.com/daleobrien/start-stop-daemon
cd start-stop-daemon
gcc start-stop-daemon.c -o start-stop-daemon
cd ../
sudo rm -R ~/start-stop-daemon
sudo mv start-stop-daemon /usr/sbin/start-stop-daemon

# data folders
mkdir /home/quantrade
mkdir /home/quantrade/data
mkdir /home/quantrade/logs
mkdir /home/quantrade/data/csv
mkdir /home/quantrade/data/customer_portfolios
mkdir /home/quantrade/data/customer_portfolios/csv
mkdir /home/quantrade/data/garch
mkdir /home/quantrade/data/garch/csv
mkdir /home/quantrade/data/indicators
mkdir /home/quantrade/data/indicators/csv
mkdir /home/quantrade/data/incoming
mkdir /home/quantrade/data/incoming/csv
mkdir /home/quantrade/data/kera
mkdir /home/quantrade/data/parquet
mkdir /home/quantrade/data/performance
mkdir /home/quantrade/data/performance/csv
mkdir /home/quantrade/data/portfolios
mkdir /home/quantrade/data/portfolios/csv
mkdir /home/quantrade/data/quandl
mkdir /home/quantrade/data/quandl/csv
mkdir /home/quantrade/data/systems
mkdir /home/quantrade/data/systems/csv

chown -R www-data:www-data ~/.plotly
sudo chown -R www-data:www-data /home/quantrade

source activate quantrade
cd /home/quantrade
pip install -r requirements.txt
