/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

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

#define SHUFFILE_SUCCESS (0)
#define SHUFFILE_FAILURE (1)

/* initialize library */
int shuffile_init();

/* shutdown library */
int shuffile_finalize();

/* associate a set of files with the calling process */
int shuffile_create(
  int numfiles,       /* number of files */
  const char** files, /* array of file names */
  const char* name    /* path name to store association information */
);

/* migrate files to owner process, if necessary */
int shuffile_dance(
  const char* name /* path name containing association information */
);

/* drop association information,
 * which is useful when cleaning up */
int shuffile_remove(
  const char* name
);

#endif /* SHUFFILE_H */
