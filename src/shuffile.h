/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
 */

#ifndef SHUFFILE_H
#define SHUFFILE_H

#include "mpi.h"
#include "kvtree.h"

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#define SHUFFILE_SUCCESS (0)
#define SHUFFILE_FAILURE (1)

/* initialize library */
int shuffile_init();

/* shutdown library */
int shuffile_finalize();

/* associate a set of files with the calling process,
 * name of process is taken as rank in comm,
 * collective across processes in comm */
int shuffile_create(
  MPI_Comm comm,       /* IN - group of processes participating in shuffle */
  MPI_Comm comm_store, /* IN - group of processes sharing a shuffle file, subgroup of comm */
  int numfiles,        /* IN - number of files */
  const char** files,  /* IN - array of file names */
  const char* name     /* IN - path name to file to store shuffle information */
);

/* migrate files to owner process, if necessary,
 * collective across processes in comm */
int shuffile_migrate(
  MPI_Comm comm,       /* IN - group of processes participating in shuffle */
  MPI_Comm comm_store, /* IN - group of processes sharing a shuffle file, subgroup of comm */
  const char* name     /* IN - path name to file containing shuffle info */
);

/* drop association information,
 * which is useful when cleaning up,
 * collective across processes in comm */
int shuffile_remove(
  MPI_Comm comm,       /* IN - group of processes participating in shuffle */
  MPI_Comm comm_store, /* IN - group of processes sharing a shuffle file, subgroup of comm */
  const char* name     /* IN - path name to file containing shuffle info */
);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* SHUFFILE_H */
