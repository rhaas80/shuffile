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
  sprintf(filename, "./testfile_%d.out", rank);
  int fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd != -1) {
    write(fd, buf, strlen(buf));
    close(fd);
  } else {
    printf("Error opening file %s: %d %s\n", filename, errno, strerror(errno));
  }

  const char* filelist[1] = { filename };

  shuffile_init();

  shuffile_create(1, filelist, "/tmp/shuffle");

  shuffile_dance("/tmp/shuffle");

  //shuffile_remove("/tmp/shuffle");

  shuffile_finalize();

  MPI_Finalize();

  return 0;
}
