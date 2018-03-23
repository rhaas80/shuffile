#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include <limits.h>
#include <unistd.h>

#include "mpi.h"

#include "shuffile.h"

int main (int argc, char* argv[])
{
  MPI_Init(&argc, &argv);

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  char buf[256];
  sprintf(buf, "data from rank %d\n", rank);

  char filename[256];
  sprintf(filename, "/tmp/moody20/testfile_%d.out", rank);
  int fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd != -1) {
    write(fd, buf, strlen(buf));
    close(fd);
  } else {
    printf("Error opening file %s: %d %s\n", filename, errno, strerror(errno));
  }

  const char* filelist[1] = { filename };

  shuffile_init();

  MPI_Comm comm_world = MPI_COMM_WORLD;
  MPI_Comm comm_store = MPI_COMM_WORLD;

  /* associate files in filelist with calling process */
  shuffile_create(comm_world, comm_store, 1, filelist, "/tmp/moody20/shuffle");

  /* migrate files back to owner process */
  shuffile_migrate(comm_world, comm_store, "/tmp/moody20/shuffle");

  /* delete association information */
  shuffile_remove(comm_world, comm_store, "/tmp/moody20/shuffle");

  /* assume running two procs per node in block distribution */
  MPI_Comm_split(comm_world, rank / 2, 0, &comm_store);

  /* reverse ranks in comm world */
  MPI_Comm comm_restart;
  MPI_Comm_split(comm_world, 0, ranks - rank, &comm_restart);

  /* associate files in filelist with calling process */
  shuffile_create(comm_world, comm_store, 1, filelist, "/tmp/moody20/shuffle");

  /* migrate files back to owner process */
  shuffile_migrate(comm_restart, comm_store, "/tmp/moody20/shuffle");

  /* delete association information */
  shuffile_remove(comm_world, comm_store, "/tmp/moody20/shuffle");

  MPI_Comm_free(&comm_store);
  MPI_Comm_free(&comm_restart);

  shuffile_finalize();

  MPI_Finalize();

  return 0;
}
