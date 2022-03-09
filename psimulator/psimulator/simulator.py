## Simulator node entry point.
## Allows concurrent execution of multiple simulations, with stdout collection.

import json
import subprocess as sp
import os
import sys
import threading

# Initialize MPI
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# MPI message codes (from root)
MSG_NEXTSIM  = 0
MSG_FINISHED = 1

print('started rank {}'.format(rank))
sys.stdout.flush()

def execute_simulation(cmd, cwd):
    """Executes a command <cmd> in working directory <cwd> and collects the
       output into "<cwd>/.simulation_stdout". """

    try:
        stdout = open(os.path.join(cwd, '.simulation_stdout'), 'w')
        sp.run(['sh', '-c', cmd], stdout=stdout, cwd=cwd).check_returncode()
        stdout.close()

        # Write OK flag to disk
        open(os.path.join(cwd, '.simulation_ok'), 'w').close()
    except:
        e = sys.exc_info()[0]
        print('Simulation {} @ {} failed: {}'.format(cmd, cwd, e))

def scheduler_main(simulations):
    print('Dispatching {} simulations to {} workers.'.format(len(simulations), size))

    for sim in simulations:
        # Wait for next request
        worker = comm.recv()

        # Dispatch simulation
        comm.send((MSG_NEXTSIM, sim), worker)

        print('dispatched {} to {}'.format(sim, worker))

    print('Terminating scheduler thread.')
    sys.stdout.flush()

    # Wait for next requests and send termination signal
    for node in range(1, size):
        comm.recv(source=node)
        comm.send((MSG_FINISHED, None), node)

# Start scheduler on root
if rank == 0:
    # Read simulation JSON from stdin
    input_bytes = sys.stdin.read()

    js = json.loads(input_bytes)

    sim_object = list(map(lambda x: tuple(x), js))

    scheduler_main(sim_object)
    sys.exit(0)

    #threading.Thread(target=scheduler_main, args=(sim_object,)).start()

# Start simulator loop
while True:
    # Send rank to root
    comm.send(rank, 0)

    # Wait for response
    msg, data = comm.recv(source=0)

    if msg == MSG_FINISHED:
        break
    elif msg == MSG_NEXTSIM:
        print('executing {}'.format(data))
        execute_simulation(data[0], data[1])
        print('finished {}'.format(data[0]))
        sys.stdout.flush()