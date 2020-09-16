#ifndef SHUFFILE_H
#define SHUFFILE_H

#include "mpi.h"

/** \defgroup shuffile Shuffile
 *  \brief Shuffle files between MPI ranks
 *
 * Files are registered with Used during restart, shuffile will move a
 * file to the 'owning' MPI rank. */

/** \file shuffile.h
 *  \ingroup shuffile
 *  \brief shuffle files between mpi ranks */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#define SHUFFILE_SUCCESS (0)
#define SHUFFILE_FAILURE (1)

#define SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE "MPI_BUF_SIZE"
#define SHUFFILE_KEY_CONFIG_DEBUG "DEBUG"

/** initialize library */
int shuffile_init();

/** shutdown library */
int shuffile_finalize();

/**
 * Get/set shuffile configuration values.
 *
 * config: The new configuration.  Global variables are in top level of
 *         the tree, and per-ID values are subtrees.  If config=NULL,
 *         then return a kvtree with all the configuration values (globals
 *         and all per-ID trees).
 *
 * Return value: If config != NULL, then return config on success.  If
 *               config=NULL (you're querying the config) then return
 *               a new kvtree on success.  Return NULL on any failures.
 */
typedef struct kvtree_struct kvtree;
kvtree* shuffile_config(
  const kvtree* config /** [IN] - kvtree of options */
);

/** associate a set of files with the calling process,
 * name of process is taken as rank in comm,
 * collective across processes in comm */
int shuffile_create(
  MPI_Comm comm,       /**< [IN] - group of processes participating in shuffle */
  MPI_Comm comm_store, /**< [IN] - group of processes sharing a shuffle file, subgroup of comm */
  int numfiles,        /**< [IN] - number of files */
  const char** files,  /**< [IN] - array of file names */
  const char* name     /**< [IN] - path name to file to store shuffle information */
);

/** migrate files to owner process, if necessary,
 * collective across processes in comm */
int shuffile_migrate(
  MPI_Comm comm,       /**< [IN] - group of processes participating in shuffle */
  MPI_Comm comm_store, /**< [IN] - group of processes sharing a shuffle file, subgroup of comm */
  const char* name     /**< [IN] - path name to file containing shuffle info */
);

/** drop association information,
 * which is useful when cleaning up,
 * collective across processes in comm */
int shuffile_remove(
  MPI_Comm comm,       /**< [IN] - group of processes participating in shuffle */
  MPI_Comm comm_store, /**< [IN] - group of processes sharing a shuffle file, subgroup of comm */
  const char* name     /**< [IN] - path name to file containing shuffle info */
);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* SHUFFILE_H */
