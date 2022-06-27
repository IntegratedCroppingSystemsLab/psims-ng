#!/bin/bash

mpirun --mca opal_warn_on_missing_libcuda 0 python -m psimulator $*
