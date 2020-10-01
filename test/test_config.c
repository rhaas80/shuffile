#include <stdio.h>
#include <stdlib.h>
#include "shuffile.h"
#include "shuffile_util.h"

#include "kvtree.h"
#include "kvtree_util.h"

int
main(int argc, char *argv[]) {
    int rc;
    kvtree* shuffile_config_values = kvtree_new();

    int old_shuffile_debug = shuffile_debug;
    int old_shuffile_mpi_buf_size = shuffile_mpi_buf_size;

    MPI_Init(&argc, &argv);

    rc = shuffile_init();
    if (rc != SHUFFILE_SUCCESS) {
        printf("shuffile_init() failed (error %d)\n", rc);
        return rc;
    }

    /* check shuffile configuration settings */
    rc = kvtree_util_set_int(shuffile_config_values, SHUFFILE_KEY_CONFIG_DEBUG,
                             !old_shuffile_debug);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        return rc;
    }

    printf("Configuring shuffile (first set of options)...\n");
    if (shuffile_config(shuffile_config_values) == NULL) {
        printf("shuffile_config() failed\n");
        return EXIT_FAILURE;
    }

    /* check options just set */

    if (shuffile_debug != !old_shuffile_debug) {
        printf("shuffile_config() failed to set %s: %d != %d\n",
               SHUFFILE_KEY_CONFIG_DEBUG, shuffile_debug, !old_shuffile_debug);
        return EXIT_FAILURE;
    }

    /* configure remainder of options */
    kvtree_delete(&shuffile_config_values);
    shuffile_config_values = kvtree_new();

    rc = kvtree_util_set_int(shuffile_config_values, SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE,
                             old_shuffile_mpi_buf_size + 1);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        return rc;
    }

    printf("Configuring shuffile (second set of options)...\n");
    if (shuffile_config(shuffile_config_values) == NULL) {
        printf("shuffile_config() failed\n");
        return EXIT_FAILURE;
    }

    /* check all options once more */

    if (shuffile_debug != !old_shuffile_debug) {
        printf("shuffile_config() failed to set %s: %d != %d\n",
               SHUFFILE_KEY_CONFIG_DEBUG, shuffile_debug, !old_shuffile_debug); 
        return EXIT_FAILURE;
    }

    if (shuffile_mpi_buf_size != old_shuffile_mpi_buf_size + 1) {
        printf("shuffile_config() failed to set %s: %d != %d\n",
               SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE, shuffile_mpi_buf_size,
               old_shuffile_mpi_buf_size); 
        return EXIT_FAILURE;
    }

    rc = shuffile_finalize();
    if (rc != SHUFFILE_SUCCESS) {
        printf("shuffile_finalize() failed (error %d)\n", rc);
        return rc;
    }

    MPI_Finalize();

    return SHUFFILE_SUCCESS;
}
