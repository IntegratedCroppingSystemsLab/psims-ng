import json
import os
from os import path

from . import sim

class Collection:
    """Manages a collection of simulations."""

    def __init__(self, dir):
        """Initializes the collection. Scans the collection directory for
           available simulations."""

        self.sims = []

        if not path.isdir(dir):
            raise RuntimeError('{} is not a directory'.format(dir))

        for entry in os.scandir(dir):
            if entry.is_dir():
                self.sims.append(sim.Simulation(entry.path))

        # find metadata if it exsits
        mdpath = path.join(dir, 'metadata.json')

        if path.isfile(mdpath):
            with open(mdpath, 'r') as f:
                self.metadata = json.loads(f.read())

        print('Found {} simulations'.format(len(self.sims)))

    def schedule(self):
        """Dispatches simulations to other MPI nodes until they have all been
           executed."""

    def simulations(self):
        return self.sims