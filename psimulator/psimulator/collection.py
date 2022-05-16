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

    def merge(self):
        db = sqlite3.connect('output.db')
        first = True
        c = db.cursor()

        for sim in self.sims:
            for out in sim.outputs():
                db.execute('ATTACH DATABASE \"{}\" as inp'.format(path.join(sim.tld, out[0])))

                c.execute('ALTER TABLE inp.Report ADD COLUMN geometry text')
                c.execute('UPDATE inp.Report SET geometry=? WHERE 1=1', [sim.geometry,])

                if first:
                    c.execute('SELECT sql FROM inp.sqlite_master WHERE type="table" AND name="Report"')
                    sql = c.fetchone()[0]
                    c.execute(sql)
                    first = False

                db.execute('INSERT INTO Report SELECT * FROM inp.Report')
                db.commit()
                print('Merged {}'.format(sim.command))
                db.execute('DETACH DATABASE inp')


