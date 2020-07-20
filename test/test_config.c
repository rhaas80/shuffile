#include <stdio.h>
#include <stdlib.h>
#include "shuffile.h"
#include "shuffile_util.h"

#include "kvtree.h"
#include "kvtree_util.h"

/* helper function to check for known options */
void check_known_options(const kvtree* configured_values,
                         const char* known_options[])
{
  /* report all unknown options (typos?) */
  const kvtree_elem* elem;
  for (elem = kvtree_elem_first(configured_values);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    const char* key = kvtree_elem_key(elem);

    /* must be only one level deep, ie plain kev = value */
    const kvtree* elem_hash = kvtree_elem_hash(elem);
    if (kvtree_size(elem_hash) != 1) {
      printf("Element %s has unexpected number of values: %d", key,
           kvtree_size(elem_hash));
      exit(EXIT_FAILURE);
    }

    const kvtree* kvtree_first_elem_hash =
      kvtree_elem_hash(kvtree_elem_first(elem_hash));
    if (kvtree_size(kvtree_first_elem_hash) != 0) {
      printf("Element %s is not a pure value", key);
      exit(EXIT_FAILURE);
    }

    /* check against known options */
    const char** opt;
    int found = 0;
    for (opt = known_options; *opt != NULL; opt++) {
      if (strcmp(*opt, key) == 0) {
        found = 1;
        break;
      }
    }
    if (! found) {
      printf("Unknown configuration parameter '%s' with value '%s'\n",
        kvtree_elem_key(elem),
        kvtree_elem_key(kvtree_elem_first(kvtree_elem_hash(elem)))
      );
      exit(EXIT_FAILURE);
    }
  }
}

/* helper function to check option values in kvtree against expected values */
void check_options(const int exp_debug, const int exp_mpi_buf_size)
{
  kvtree* config = shuffile_config(NULL);
  if (config == NULL) {
    printf("shuffile_config failed\n");
    exit(EXIT_FAILURE);
  }

  int cfg_debug;
  if (kvtree_util_get_int(config, SHUFFILE_KEY_CONFIG_DEBUG, &cfg_debug) !=
    KVTREE_SUCCESS)
  {
    printf("Could not get %s from shuffile_config\n",
           SHUFFILE_KEY_CONFIG_DEBUG);
    exit(EXIT_FAILURE);
  }
  if (cfg_debug != exp_debug) {
    printf("shuffile_config returned unexpected value %d for %s. Expected %d.\n",
           cfg_debug, SHUFFILE_KEY_CONFIG_DEBUG,
           exp_debug);
    exit(EXIT_FAILURE);
  }

  int cfg_mpi_buf_size;
  if (kvtree_util_get_int(config, SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE,
                          &cfg_mpi_buf_size) != KVTREE_SUCCESS)
  {
    printf("Could not get %s from shuffile_config\n",
           SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE);
    exit(EXIT_FAILURE);
  }
  if (cfg_mpi_buf_size != exp_mpi_buf_size) {
    printf("shuffile_config returned unexpected value %d for %s. Expected %d.\n",
           cfg_mpi_buf_size, SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE,
           exp_mpi_buf_size);
    exit(EXIT_FAILURE);
  }

  static const char* known_options[] = {
    SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE,
    SHUFFILE_KEY_CONFIG_DEBUG,
    NULL
  };
  check_known_options(config, known_options);

  kvtree_delete(&config);
}

int
main(int argc, char *argv[]) {
    int rc;
    kvtree* shuffile_config_values = kvtree_new();

    MPI_Init(&argc, &argv);

    rc = shuffile_init();
    if (rc != SHUFFILE_SUCCESS) {
        printf("shuffile_init() failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }

    /* needs to be afater shuffile_init to catch initialization of globals */
    int old_shuffile_debug = shuffile_debug;
    int old_shuffile_mpi_buf_size = shuffile_mpi_buf_size;

    int new_shuffile_debug = !old_shuffile_debug;
    int new_shuffile_mpi_buf_size = old_shuffile_mpi_buf_size + 1;

    check_options(old_shuffile_debug, old_shuffile_mpi_buf_size);

    /* check shuffile configuration settings */
    rc = kvtree_util_set_int(shuffile_config_values, SHUFFILE_KEY_CONFIG_DEBUG,
                             new_shuffile_debug);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }

    printf("Configuring shuffile (first set of options)...\n");
    if (shuffile_config(shuffile_config_values) == NULL) {
        printf("shuffile_config() failed\n");
        return EXIT_FAILURE;
    }

    /* check options just set */

    if (shuffile_debug != new_shuffile_debug) {
        printf("shuffile_config() failed to set %s: %d != %d\n",
               SHUFFILE_KEY_CONFIG_DEBUG, shuffile_debug, new_shuffile_debug);
        return EXIT_FAILURE;
    }

    check_options(new_shuffile_debug, old_shuffile_mpi_buf_size);

    /* configure remainder of options */
    kvtree_delete(&shuffile_config_values);
    shuffile_config_values = kvtree_new();

    rc = kvtree_util_set_int(shuffile_config_values, SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE,
                             new_shuffile_mpi_buf_size);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }

    printf("Configuring shuffile (second set of options)...\n");
    if (shuffile_config(shuffile_config_values) == NULL) {
        printf("shuffile_config() failed\n");
        return EXIT_FAILURE;
    }

    /* check all options once more */

    if (shuffile_debug != new_shuffile_debug) {
        printf("shuffile_config() failed to set %s: %d != %d\n",
               SHUFFILE_KEY_CONFIG_DEBUG, shuffile_debug, new_shuffile_debug);
        return EXIT_FAILURE;
    }

    if (shuffile_mpi_buf_size != new_shuffile_mpi_buf_size) {
        printf("shuffile_config() failed to set %s: %d != %d\n",
               SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE, shuffile_mpi_buf_size,
               old_shuffile_mpi_buf_size); 
        return EXIT_FAILURE;
    }

    check_options(new_shuffile_debug, new_shuffile_mpi_buf_size);

    rc = shuffile_finalize();
    if (rc != SHUFFILE_SUCCESS) {
        printf("shuffile_finalize() failed (error %d)\n", rc);
        return EXIT_FAILURE;
    }

    MPI_Finalize();

    return EXIT_SUCCESS;
}
