import json
import os
from os import path
import sqlite3

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

    def simulations(self):
        return self.sims

    def merge(self, target):
        db = sqlite3.connect(target)
        c = db.cursor()

        initialized = {}

        for sim in self.sims:
            for out in sim.outputs():
                db.execute('ATTACH DATABASE \"{}\" as inp'.format(path.join(sim.tld, out.path)))

                for target in out.targets:
                    c.execute('ALTER TABLE inp.{} ADD COLUMN geometry TEXT'.format(target))
                    c.execute('UPDATE inp.{} SET geometry=? WHERE 1=1'.format(target), [sim.geometry,])

                    if target not in initialized:
                        c.execute('SELECT sql FROM inp.sqlite_master WHERE type="table" AND name=?', [target,])
                        sql = c.fetchone()[0]
                        c.execute(sql)
                        initialized[target] = True

                    db.execute('INSERT INTO {} SELECT * FROM inp.{}'.format(target, target))
                    print('Merged {} : {}'.format(sim.tld, target))

                db.commit()
                db.execute('DETACH DATABASE inp')
