#!/usr/bin/env bash

sudo apt install gcc -y
git clone https://github.com/daleobrien/start-stop-daemon
cd start-stop-daemon
gcc start-stop-daemon.c -o start-stop-daemon
sudo mv start-stop-daemon /usr/sbin/start-stop-daemon
