The simulator application expects a database input describing the simulations to be executed.
This will replace/merge with the existing scheduler.

==== INPUT database ====

The input database file should be an sqlite3 database with the following table:

    Simulations (
        name TEXT,       # Name of simulation (REQUIRED)
        outputs TEXT,    # Simulation output tables (REQUIRED), see OUTPUTS specification below
        command TEXT,    # Simulation command (REQUIRED)
        environment BLOB # TAR archive of working directory for simulation

        (any further fields are optional)

        longitude FLOAT, # Simulation location longitude
        latitude FLOAT,  # Simulation location latitude
    )

The simulations program will add the following tables to the input database:
    <NAME>_<TABLE>: Simulation <NAME>, output table <TABLE>.

The columns in the output tables are determined by the simulation output. For CSV tables, all columns
are of type TEXT.

==== OUTPUTS ====

    The 'outputs' column describes how the output from the simulation should be interpreted.
    The context of this column is formatted similar to a unix PATH:

        OUTFILE,TYPE,TABLENAME:OUTFILE2,TYPE2,TABLENAME2 (:...)

    OUTFILE should point to an output file relative to the simulation directory.
    TYPE must be one of 'sqlite' or 'csv'.


CSV:
    TABLENAME will be the name of table created in the database (after being appended to 'SIMNAME_').

SQlite:
    All tables will be imported, with the resulting name
    SIMNAME_<INNER_TABLE_NAME>

    The TABLENAME field can be omitted for sqlite outputs.
