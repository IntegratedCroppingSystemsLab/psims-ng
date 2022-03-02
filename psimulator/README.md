The simulator application expects a database input describing the simulations to be executed.
This will replace/merge with the existing scheduler.

The input database file should be an sqlite3 database with the following table:

    Simulations (
        name TEXT,       # Name of simulation (REQUIRED)
        command TEXT,    # Simulation command (REQUIRED)

        # TODO: solidify expected fields.

        simtype TEXT,    # Simulation type (Apsim, ApsimNG, DSAT)
        longitude FLOAT, # Simulation location longitude
        latitude FLOAT,  # Simulation location latitude
    )

The simulations program will produce an output database containing the following tables

    Simulations (as copied from the input file)
    Simulation_NAME: Output table correspoding to the simulation with name NAME.

    The columns available in Simulation_NAME depend on the output format from the executed simulatoin and may vary.
