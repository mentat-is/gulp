#!/bin/bash

# set up permissions for docker socket
sudo chmod 666 /var/run/docker.sock

# install development packages
pip3 install -e .
pip3 install -e ./muty-python

echo "development environment setup complete"
