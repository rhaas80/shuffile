# Overview

[![Build Status](https://api.travis-ci.org/ECP-VeloC/shuffile.png?branch=master)](https://travis-ci.org/ECP-VeloC/shuffile)

This module lets one associate a set of files with a process name.
Currently, the name is implied to be the rank within MPI_COMM_WORLD.
In the event that a set of distributed processes are moved, for example
when restarting an MPI job, functions will migrate files from their original
locations to the new locations where the processes are running.

Usage is documented in [src/shuffile.h]() and the [User API docs](https://ecp-veloc.github.io/component-user-docs/group__shuffile.html).

# Building

To build KVTree:

    git clone git@github.com:LLNL/KVTree.git KVTree.git

    mkdir build
    mkdir install

    cd build
    cmake -DCMAKE_INSTALL_PREFIX=../install -DMPI=ON ../KVTree.git
    make clean
    make
    make install

To build shuffile:

    cmake -DCMAKE_BUILD_TYPE=Debug -DWITH_KVTREE_PREFIX=`pwd`/install .
    make

# Testing
Some simple test programs exist in the test directory that are run by the `test` target.

    make test

## Release

Copyright (c) 2018, Lawrence Livermore National Security, LLC.
Produced at the Lawrence Livermore National Laboratory.
<br>
Copyright (c) 2018, UChicago Argonne LLC, operator of Argonne National Laboratory.


For release details and restrictions, please read the [LICENSE]() and [NOTICE]() files.

`LLNL-CODE-751725` `OCEC-18-060`
