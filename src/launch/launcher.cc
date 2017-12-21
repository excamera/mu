#include <cstdlib>
#include <iostream>
#include <string>
#include "launch.hh"

int main(int argc, char *argv[]) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <listen_addr>" << std::endl;
        exit(-1);
    }
    servegrpc(std::string(argv[1]));
}
