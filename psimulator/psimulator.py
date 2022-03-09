#!/usr/bin/python
# Simulator entry point.

import sys

if len(sys.argv) < 2:
    print('usage: {} DATABASE'.format(sys.argv[0]))
    sys.exit(-1)

import psimulator
psimulator.run(sys.argv[1])
