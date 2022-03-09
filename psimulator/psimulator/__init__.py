import json
import io
import os
import sqlite3
import subprocess as sp
import tarfile
import tempfile

def run_simulations(simulation_list):
    """Executes multiple simulations and returns the standard output of each.
       simulation_list must be a list containing tuples for each simulation
       of the format (simulationCmd, simulationCwd) where simulationCmd
       denotes the command to be executed (via sh) and simulationCwd denotes
       the working directory in which to execute the command.
       
       A list of byte arrays is returned containing the standard output of each
       simulation. Any output to standard error will be directed to the stderr
       stream of the caller."""

    encoded = json.dumps(simulation_list).encode('utf-8')
    simulator_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'simulator.py')

    try:
        sp.run(['mpiexec', 'python', simulator_path], input=encoded).check_returncode()
    except sp.CalledProcessError as e:
        print('Simulator process failed: {}'.format(str(e)))

def run(database):
    print('Starting simulator on database {}'.format(database))

    # Connect to input database
    conn = sqlite3.connect(database)

    # Read simulation list
    cursor = conn.cursor()
    cursor.execute('SELECT name, outputs, command, environment FROM Simulations')

    sims_raw = cursor.fetchall()

    print('Found {} simulations'.format(len(sims_raw)))

    # Unpack simulation environments
    print('Extracting simulation environments..')

    def initialize_simulation(row):
        dst = tempfile.mkdtemp()
        with tarfile.open(fileobj=io.BytesIO(row[3])) as tf:
            tf.extractall(path=dst)
        return (row[0], row[1], row[2], dst)

    simulations = list(map(initialize_simulation, sims_raw))

    # Pass cmd, cwd to simulaton runner
    print('Executing simulations..')
    run_simulations(list(map(lambda row: (row[2], row[3]), simulations)))

    # Walk over simulation results
    print('Merging simulation results..')

    # Walk through simulations
    for sim in simulations:
        # Walk through each output file
        outputs = sim[1].split(':')

        for output in outputs:
            fields = output.split(',')

            outfile = os.path.join(sim[3], fields[0])
            outtype = fields[1]

            if outtype == 'sqlite':
                # TODO: might be a way to enumerate tables through attached database,
                # seems to not work right now though.

                # Query result table names
                tmpconn = sqlite3.connect(outfile)
                tmpcurs = tmpconn.cursor()

                tmpcurs.execute('SELECT name FROM sqlite_master WHERE type="table"')
                rows = tmpcurs.fetchall()

                print('Found {} result tables in {}:{}'.format(rows, sim[0], outfile))
                tmpconn.close()

                cursor.execute('ATTACH DATABASE \"{}\" AS {}'.format(outfile, sim[0]))

                for row in rows:
                    cursor.execute('CREATE TABLE {} AS SELECT * FROM {}.{}'.format(sim[0] + '_' + row[0], sim[0], row[0]))
                    print('Merged table {}'.format(sim[0] + '_' + row[0]))

                cursor.execute('DETACH DATABASE {}'.format(sim[0]))
            elif outtype == 'csv':
                if len(fields) != 3:
                    raise RuntimeError('Invalid output file field')

                outname = fields[2]

                raise RuntimeError('TODO: CSV output is not supported yet')
    
    print('Committing results to database..')
    conn.commit()
