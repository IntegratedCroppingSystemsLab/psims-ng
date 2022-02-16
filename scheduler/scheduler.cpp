#include <iostream>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include <mpi.h>

#define MSG_NEXTSIM  0
#define MSG_COMPLETE 1

using namespace std;

int main(int argc, char** argv)
{
    int c, rank, nodes;
    bool running;
    vector<pair<string, string>> sims; // (simoutpath, simcmd)

    // Parse arguments
    while ((c = getopt(argc, argv, "h")) != -1)
    {
        switch (c)
        {
            case 'h':
                cout << "usage: " << argv[0] << " [-h]" << endl;
                return 0;
            default:
                cerr << "error: unknown flag " << (char) c << endl;
                return -1;
        }
    }

    // Initialize MPI
    if (MPI_Init(&argc, &argv) != MPI_SUCCESS)
        throw runtime_error("couldn't initialize MPI");

    // Determine node ID
    if (MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS)
        throw runtime_error("couldn't query MPI rank");

    // Determine available nodes
    if (MPI_Comm_size(MPI_COMM_WORLD, &nodes) != MPI_SUCCESS)
        throw runtime_error("error: couldn't query MPI nodecount");

    // Set running flag
    running = true;

    // MPI message helpers
    auto write_int = [=](int val, int dst) {
        if (MPI_Send(&val, 1, MPI_INT, dst, 0, MPI_COMM_WORLD) != MPI_SUCCESS)
            throw runtime_error(string("error [") + to_string(rank) + "]: MPI_Send failed");
    };

    auto recv_int = [=](int src) -> int {
        int res;

        if (MPI_Recv(&res, 1, MPI_INT, src, 0, MPI_COMM_WORLD, nullptr) != MPI_SUCCESS)
            throw runtime_error(string("error [") + to_string(rank) + "]: MPI_Recv failed");

        return res;
    };

    auto write_str = [=](string val, int dst) {
        int len = val.size();

        if (MPI_Send(&len, 1, MPI_INT, dst, 0, MPI_COMM_WORLD) != MPI_SUCCESS)
            throw runtime_error(string("error [") + to_string(rank) + "]: MPI_Send failed");

        if (MPI_Send(&val[0], len, MPI_CHAR, dst, 0, MPI_COMM_WORLD) != MPI_SUCCESS)
            throw runtime_error(string("error [") + to_string(rank) + "]: MPI_Send failed");
    };

    auto recv_str = [=](int src) -> string {
        int len;

        if (MPI_Recv(&len, 1, MPI_INT, src, 0, MPI_COMM_WORLD, nullptr) != MPI_SUCCESS)
            throw runtime_error(string("error [") + to_string(rank) + "]: MPI_Recv failed");

        char* buf = new char[len + 1];

        if (MPI_Recv(buf, len, MPI_CHAR, src, 0, MPI_COMM_WORLD, nullptr) != MPI_SUCCESS)
            throw runtime_error(string("error [") + to_string(rank) + "]: MPI_Recv failed");

        buf[len] = '\0';
        string val = buf;

        delete[] buf;
        return val;
    };

    // Perform root actions
    if (!rank)
    {
        cout << nodes << " available nodes" << endl;

        // For now, treat the root node as a pure scheduling node and the others as workers.
        // This is slightly less efficient as it only effectively utilizes n-1 of the available nodes
        // In the future the root node should have a seperate scheduling thread, ideally avoiding
        // MPI messaging with itself. Then the scheduling thread will need to specialize comm
        // with the root node.
        
        // Read input simulation commands
        for (string line; getline(cin, line, '\n');)
        {
            // Find sim name
            int sep;

            if ((sep = line.find(' ')) >= line.size())
                throw runtime_error("invalid sim line " + line);

            // Split line into parts
            sims.push_back(make_pair(
                line.substr(0, sep),
                line.substr(sep + 1)
            ));
        }

        cout << "[root] scheduling " << sims.size() << " simulations" << endl;

        // Wait for sim requests
        while (sims.size())
        {
            // Receive request identity
            int target_ident = recv_int(MPI_ANY_SOURCE);

            write_int(MSG_NEXTSIM, target_ident);
            write_str(sims.back().first, target_ident);
            write_str(sims.back().second, target_ident);

            sims.pop_back();
        }

        cout << "[root] dispatched all simulations, waiting for workers" << endl;

        // Send termination msg
        for (int i = 1; i < nodes; ++i)
            write_int(MSG_COMPLETE, i);
    } else
    {
        while (running)
        {
            // Send identity
            write_int(rank, 0);

            // Wait for mode
            int simresp = recv_int(0);

            switch (simresp)
            {
                case MSG_NEXTSIM:
                    {
                        // Collect simulation info
                        string simoutpath = recv_str(0);
                        string simcmd = recv_str(0);

                        ofstream simout(simoutpath);

                        if (!simout)
                            throw runtime_error("error[" + to_string(rank) + "]: couldn't open " + simoutpath + " for writing");

                        // Execute simulation command
                        cout << "[" << rank << "]: executing simulation " << simcmd << " : " << simoutpath << endl;

                        FILE* fsim = popen(simcmd.c_str(), "r");

                        char buf[256];
                        int rbytes;

                        while ((rbytes = fread(buf, 1, sizeof buf - 1, fsim)))
                        {
                            buf[rbytes] = '\0';
                            simout << string(buf);

                            if (rbytes != sizeof buf - 1)
                                break;
                        }

                        if (pclose(fsim) < 0)
                            cerr << "warning [" << rank << "]: pclose() returned failure status for simcmd " << simcmd << endl;

                        cout << "[" << rank << "]: completed simulation " << simcmd << " : " << simoutpath << endl;
                    }
                    break;
                case MSG_COMPLETE:
                    // No more sims, go and and terminate
                    running = false;
                    break;
                default:
                    throw runtime_error("error [" + to_string(rank) + "]: unexpected message type " + to_string(simresp));
            }
        }
    }

    // Cleanup MPI
    if (MPI_Finalize() != MPI_SUCCESS)
        cerr << "warning: MPI_Finalize() failed" << endl;

    return 0;
}
