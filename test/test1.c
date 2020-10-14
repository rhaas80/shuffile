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

#define TEST_PASS (0)
#define TEST_FAIL (1)

int main (int argc, char* argv[])
{
  int rc = TEST_PASS;
  MPI_Init(&argc, &argv);

  int rank, ranks, i;
  int comm_restart_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  char buf[256];
  void* buff[256];
  sprintf(buf, "data from rank %d", rank);

  char filename[256];
  sprintf(filename, "/dev/shm/testfile_%d.out", rank);
  printf("filename = /dev/shm/testfile_%d.out\n", rank);
  errno = 0;
  int fd = open(filename, O_WRONLY | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd != -1) {
    write(fd, buf, strlen(buf));
  printf("data = %s, rank=%d\n", buf,rank);
    close(fd);
  } else {
    printf("Error opening write file %s: %d %s\n", filename, errno, strerror(errno));
    rc = TEST_FAIL;
  }
  errno = 0;
  int fdr = open(filename, O_RDONLY);
  if (fdr != -1) {
    int numBytes = read(fdr, buff, 100);
    char* cbuff = (char*)buff;
    cbuff[numBytes]  = '\0';
    if(numBytes != strlen(buf)){
      printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
      printf("wrote %d bytes to file, but read %d bytes from file\n", strlen(buf), numBytes);
      return TEST_FAIL;
    }
    else{
      for(i = 0; i < numBytes; i++){
        if(buf[i] != cbuff[i]){
          printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
          printf("%dth character writtten to file was %c, but %c was read from file\n",i, buf[i], cbuff[i]);
          return TEST_FAIL;
        }
      }
    }
    printf("After first migrate, READ IN %d bytes\n", numBytes);
    printf("After first migrate, READ IN %s\n", cbuff);
    printf("data = %s, rank=%d\n", buf,rank);
    close(fdr);
  } else {
    printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
    printf("Error opening read file %s: %d %s\n", filename, errno, strerror(errno));
    return TEST_FAIL;
  }

  const char* filelist[1] = { filename };

  shuffile_init();

  MPI_Comm comm_world = MPI_COMM_WORLD;
  MPI_Comm comm_store = MPI_COMM_WORLD;

  /* associate files in filelist with calling process */
  shuffile_create(comm_world, comm_store, 1, filelist, "/dev/shm/shuffle");

  /* migrate files back to owner process */
  shuffile_migrate(comm_world, comm_store, "/dev/shm/shuffle");
     
  /* delete association information */
  shuffile_remove(comm_world, comm_store, "/dev/shm/shuffle");

  /* assume running two procs per node in block distribution */
  MPI_Comm_split(comm_world, rank / 2, 0, &comm_store);

  /* reverse ranks in comm world */
  MPI_Comm comm_restart;
  MPI_Comm_split(comm_world, 0, ranks - rank, &comm_restart);

  /* associate files in filelist with calling process */
  shuffile_create(comm_world, comm_store, 1, filelist, "/dev/shm/shuffle");

  /* migrate files back to owner process */
  shuffile_migrate(comm_restart, comm_store, "/dev/shm/shuffle");

  //sprintf(filename, "/dev/shm/testfile_%d.out", ranks-rank/2);
  //sprintf(buf, "data from rank %d", ranks-rank/2);
  MPI_Comm_rank(comm_restart, &comm_restart_rank);
  sprintf(filename, "/dev/shm/testfile_%d.out", comm_restart_rank);
  sprintf(buf, "data from rank %d", comm_restart_rank);
  errno = 0;
  fdr = open(filename, O_RDONLY);
  if (fdr != -1) {
    int numBytes = read(fdr, buff, 100);
    char* cbuff = (char*)buff;
    cbuff[numBytes]  = '\0';
    if(numBytes != strlen(buf)){
      printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
      printf("wrote %d bytes to file, but read %d bytes from file\n", strlen(buf), numBytes);
      return TEST_FAIL;
    }
    else{
      for(i = 0; i < numBytes; i++){
        if(buf[i] != cbuff[i]){
          printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
          printf("%dth character writtten to file was %c, but %c was read from file\n",i, buf[i], cbuff[i]);
          return TEST_FAIL;
        }
      }
    }
    printf("After second migrate, READ IN %d bytes\n", numBytes);
    printf("After second migrate, READ IN %s\n", cbuff);
    printf("data = %s, rank=%d\n", buf,rank);
    close(fdr);
  } else {
    printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
    printf("Error opening read file %s: %d %s\n", filename, errno, strerror(errno));
    return TEST_FAIL;
  }
  /* delete association information */
  shuffile_remove(comm_world, comm_store, "/dev/shm/shuffle");

  MPI_Comm_free(&comm_store);
  MPI_Comm_free(&comm_restart);

  //null tests
  int rc_null;
  rc_null = shuffile_create(comm_world, comm_store, 1, filelist, NULL);
  if(rc_null == SHUFFILE_SUCCESS){
    printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
    printf("shuffile_create succeded with NULL name parameter\n");
    return TEST_FAIL;
  }

  rc_null = shuffile_migrate(comm_world, comm_store, NULL);
  if(rc_null == SHUFFILE_SUCCESS){
    printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
    printf("shuffile_migrate succeded with NULL name parameter\n");
    return TEST_FAIL;
  }

  rc_null = shuffile_remove(comm_world, comm_store, NULL);
  if(rc_null == SHUFFILE_SUCCESS){
    printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
    printf("shuffile_remove succeded with NULL name parameter\n");
    return TEST_FAIL;
  }

  //MPI_COMM_NULL tests
  rc_null = shuffile_create(MPI_COMM_NULL, MPI_COMM_NULL, 1, filelist, "/dev/shm/shuffle");
  if(rc_null == SHUFFILE_SUCCESS){
    printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
    printf("shuffile_create succeded with MPI_COMM_NULL comm parameters\n");
    return TEST_FAIL;
  }
  rc_null = shuffile_migrate(MPI_COMM_NULL, MPI_COMM_NULL, "/dev/shm/shuffle");
  if(rc_null == SHUFFILE_SUCCESS){
    printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
    printf("shuffile_migrate succeded with MPI_COMM_NULL comm parameters\n");
    return TEST_FAIL;
  }
  rc_null = shuffile_remove(MPI_COMM_NULL, MPI_COMM_NULL, "/dev/shm/shuffle");
  if(rc_null == SHUFFILE_SUCCESS){
    printf ("Error in line %d, file %s, function %s.\n", __LINE__, __FILE__, __func__);
    printf("shuffile_remove succeded with MPI_COMM_NULL comm parameters\n");
    return TEST_FAIL;
  }

  shuffile_finalize();

  MPI_Finalize();

  return rc;
}
