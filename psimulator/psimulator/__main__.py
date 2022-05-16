import json
import io
from mpi4py import MPI
import os
import sqlite3
import subprocess as sp
import sys
import tarfile
import tempfile

from . import collection

if len(sys.argv) < 2:
    print('usage: {} PATH'.format(sys.argv[0]))
    sys.exit(-1)

# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# MPI message codes (from root)
MSG_NEXTSIM  = 0
MSG_FINISHED = 1

# If not scheduling node, wait for simulations and then exit.
if rank != 0:
    print('{}: ready, waiting for simulations'.format(rank))

    while True:
        # Send rank to root
        comm.send(rank, 0)

        # Wait for response
        msg, data = comm.recv(source=0)

        if msg == MSG_FINISHED:
            break
        elif msg == MSG_NEXTSIM:
            print('{}: executing {}'.format(rank, data.tld))
            data.execute()
            print('{}: finished {}'.format(rank, data.tld))
            sys.stdout.flush()

    sys.exit(0)

active = {}

# Otherwise, generate the collection and start dispatching simulations.
coll = None

try:
    coll = collection.Collection(sys.argv[1])
except Exception as e:
    for node in range(1, size):
        comm.recv(source=node)
        comm.send((MSG_FINISHED, None), node)
    print('ERROR: collection init failed: {}'.format(e))
    sys.exit(-1)

for sim in coll.simulations():
    # Wait for next request
    worker = comm.recv()

    # Dispatch simulation
    comm.send((MSG_NEXTSIM, sim), worker)

    active[worker] = True

    print('dispatched {} to {}'.format(sim.tld, worker))

sys.stdout.flush()

# Wait for next requests and send termination signal
for node in range(1, size):
    comm.recv(source=node)
    comm.send((MSG_FINISHED, None), node)

# Simulations done.
print('Finished.')

coll.merge()
