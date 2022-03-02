#!/usr/bin/python
# Simulator entry point.

import sys

if len(sys.argv) < 3:
    print('usage: {} INPUT_DB OUTPUT_DB'.format(sys.argv[0]))
    sys.exit(-1)

import psimulator
psimulator.run(sys.argv[1], sys.argv[2])
