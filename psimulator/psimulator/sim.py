import json
import os
from os import path
import subprocess as sp

class Simulation:
    """Manages the lifecycle of a single simulation. Each Simulation
       corresponds to a directory containing a 'simulation.json' and all of the
       files necessary to execute the simulation.

       'simulation.json' must be of the following format:

       {
         "command": "<command-to-execute-simulation>",
         "geometry": "<well-known-text geometry of simulation>",
         "output": [
            {
              "path": "<relative-path-to-output-file>",
              "type": "sqlite",
              "targets": ["<table1>", "<table2>", ...],
            },
            ...
         ]
       }

       """

    def __init__(self, tld):
        """Initializes a simulation. Expects the path to the top-level
           directory."""

        self.tld = tld

        # check path exists
        if not path.isdir(tld):
            raise RuntimeError('simulation directory {} not found'.format(tld))

        # initialize metadata
        mdpath = path.join(tld, 'simulation.json')

        if not path.isfile(mdpath):
            raise RuntimeError('required metadata {} not found'.format(mdpath))

        with open(path.join(tld, 'simulation.json'), 'r') as f:
            self.metadata = json.loads(f.read())

        # verify required fields
        if 'command' not in self.metadata or type(self.metadata['command']) is not str:
            raise RuntimeError('{}: missing required field "command"'.format(mdpath))

        if 'outputs' not in self.metadata:
            raise RuntimeError('{}: missing required field "outputs"'.format(mdpath))

        if 'geometry' not in self.metadata:
            raise RuntimeError('{}: missing required field "geometry"'.format(mdpath))

        self.command = self.metadata['command']
        self.geometry = self.metadata['geometry']
        self.outputs = []

        for out in self.metadata['outputs']:
            if 'path' not in out:
                raise Exception('missing required field "path" in output')

            if 'targets' not in out:
                raise Exception('missing required field "targets" in output')

            nextout = lambda: None
            nextout.path = out['path']
            nextout.targets = out['targets']

            self.outputs.append(nextout)

    def execute(self):
        """Executes this simulation. Returns true if the simulation succeeds,
           and false otherwise."""

        pstdout = path.join(self.tld, '.stdout')
        pstderr = path.join(self.tld, '.stderr')

        stdout = open(pstdout, 'w')
        stderr = open(pstderr, 'w')

        try:
            sp.run(['sh', '-c', self.command], stdout=stdout, stderr=stderr, cwd=self.tld).check_returncode()
        except sp.CalledProcessError:
            print('ERROR: simulation {} failed with nonzero return code; see output files\n\t{}\n\t{}'
                  .format(self.tld, pstdout, pstderr))
            return False
        finally:
            stdout.close()
            stderr.close()

        # verify outputs were correctly generated 
        for of in self.outputs():
            opath = path.join(self.tld, of.path)

            if not path.exists(opath):
                print('WARNING: simulation {} succeeded but output file {} was not generated'
                      .format(self.tld, opath))

        return True
