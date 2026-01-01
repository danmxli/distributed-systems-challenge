#!/bin/bash

# maelstrom dependencies
sudo apt-get update && sudo apt-get install -y graphviz gnuplot

# get v0.2.3 from github releases
wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2
tar -xvf maelstrom.tar.bz2

sudo mv maelstrom /opt/maelstrom
rm maelstrom.tar.bz2

# export maelstrom to path, run source ~/.bashrc to apply changes
echo 'export PATH=$PATH:/opt/maelstrom' >> ~/.bashrc