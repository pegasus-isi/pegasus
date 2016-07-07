/* MPI hello world */
/* Based on http://www.lam-mpi.org/tutorials/one-step/ezstart.php  */

#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#define MAXSTR 4096
#define WORKTAG 1
#define DIETAG 2


/* Local functions */
static void usage( char** );
static void master( int );
static void worker( int );

static char* outputFile = NULL;

int main(int argc, char **argv){
  int rank;
  char buffer[MAXSTR];

  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if( rank == 0 ){
    /* rank 0 is master process */
    /* parse command line arguments */
    int    c;
    FILE   *fin;
    char *cwd;
    while ((c = getopt(argc, argv, "ho:")) != -1) {
      switch (c) {    
         case 'o':
	  outputFile = optarg;
	  break;

        default: //similar to h
	  usage( argv );
	  break;
      }
    }
    if (optind < argc) {
      fprintf( stderr, "[Master] non-option options passed on command line to %s", argv[0]);
      while (optind < argc)
	fprintf ( stderr, "%s ", argv[optind++]);
      fprintf ( stderr, "\n");
      return 1;
    }
  
    if( outputFile == NULL){
      fprintf( stderr, "[Master] output file is a required option\n");
      usage( argv );
      return 1;
    }
    fprintf( stdout, "[Master] output file passed on command line %s ", outputFile );
    cwd = getcwd( NULL, 2048 );
    fprintf( stdout, "[Master] job was launched in directory %s ", cwd );
    master( rank );
  }
  else{
    worker( rank );
  }

  //shutdown MPI
  MPI_Finalize();
  return 0;
}

static void usage( char** argv ){
  fprintf( stderr, "Usage: %s [-h] [-o output-file]\n", argv[0]);
}

static void master(int myrank){
  int nprocesses, rank;
  char result[MAXSTR];
  MPI_Status status;
  char hostname[256];
  char work[MAXSTR];
  FILE *output;
  gethostname(hostname,255);

  /* Find out how many processes there are in the default
     communicator */
  MPI_Comm_size(MPI_COMM_WORLD, &nprocesses);

  fprintf( stdout, "[Master] Total number of MPI processes %d\n", nprocesses);
  fprintf( stdout, "[Master] Hello world!  I am process number: %d on host %s\n", myrank, hostname);
  sprintf( work,"Message from the master\n");

  /* Seed the slaves; send one unit of work to each slave. */
  for (rank = 1; rank < nprocesses; ++rank) {

    /* Find the next item of work to do */
    //not sending any work right now.
    //just capturing the outputs
    //work = get_next_work_item();

    /* Send it to each rank */
    MPI_Send(&work,             /* message buffer */
             strlen(work)+1,    /* one data item */
             MPI_CHAR,           /* data item is a string */
             rank,              /* destination process rank */
             WORKTAG,           /* user chosen message tag */
             MPI_COMM_WORLD);   /* default communicator */
  }

  /* Loop over till we hear back from all the slaves */
  output = fopen( outputFile,"w" );
  fprintf( output, "[Master] Total number of MPI processes %d\n", nprocesses);
  fprintf( output, "[Master] Hello world!  I am process number: %d on host %s\n", myrank, hostname);
  rank = 1;
  while( rank < nprocesses ){

    /* Receive results from a slave */
    MPI_Recv(&result,           /* message buffer */
             MAXSTR,                 /* one data item */
             MPI_CHAR,        /* of type string */
             MPI_ANY_SOURCE,    /* receive from any sender */
             MPI_ANY_TAG,       /* any type of message */
             MPI_COMM_WORLD,    /* default communicator */
             &status);          /* info about the received message */
    printf(  "[Master %d] Received Message from worker\n", myrank);
    printf(  "[Master] %s\n",  result);

    /*write to output file*/
    fputs ( result, output );
    rank++;
  }

  /* close the output file*/
  fclose ( output );

  /* There's no more work to be done, so receive all the outstanding
     results from the slaves. */
  /*
  for (rank = 1; rank < ntasks; ++rank) {
    MPI_Recv(&result, 1, MPI_DOUBLE, MPI_ANY_SOURCE,
             MPI_ANY_TAG, MPI_COMM_WORLD, &status);
  }
  */

  /* Tell all the slaves to exit by sending an empty message with the
     DIETAG. */
  printf(  "[Master] Sending DIE messages to workers \n");
  for (rank = 1; rank < nprocesses; ++rank) {
    MPI_Send(0, 0, MPI_CHAR, rank, DIETAG, MPI_COMM_WORLD);
  }
}

static void worker(int myrank){
  MPI_Status status;
  char buffer[MAXSTR];
  char work[MAXSTR];
  char hostname[255];
  gethostname(hostname,255);
  //printf(  "[Worker] Hello world!  I am process number: %d on host %s\n", myrank, hostname);
  
  while (1) {

    /* Receive a message from the master */

    MPI_Recv(&work, MAXSTR, MPI_CHAR, 0, MPI_ANY_TAG,
             MPI_COMM_WORLD, &status);
    printf(  "[Worker %d] Received Message\n", myrank);

    /* Check the tag of the received message. */

    if (status.MPI_TAG == DIETAG) {
       printf(  "[Worker %d] Exiting\n", myrank);
      return;
    }

    /* Do the work */
    //printf(  "[Worker] Hello world!  I am process number: %d on host %s\n", myrank, hostname);
    sprintf( buffer,"[Worker] Hello world!  I am process number: %d on host %s\n", myrank, hostname);


    /* Send the result back */
    printf(  "[Worker %d] Sending Message %s\n", myrank, buffer);
    MPI_Send(&buffer, strlen(buffer)+1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
  }
}


