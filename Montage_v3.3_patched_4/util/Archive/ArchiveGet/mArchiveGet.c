/* Module: mArchiveGet.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.1      John Good        14Feb05  Got rid of IRSA-specific code
1.0      John Good        15Dec04  Baseline code

*/
 
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <errno.h>
#include <montage.h>

#define MAXLEN 16384

extern char *optarg;
extern int optind, opterr;

extern int getopt(int argc, char *const *argv, const char *options);

int tcp_connect(char *hostname, int port);

int parseUrl(char *urlStr, char *hostStr, int *port, char **dataref);

int debug;


/*************************************************************************/
/*                                                                       */
/*  mArchiveGet                                                          */
/*                                                                       */
/*  Montage is a set of general reprojection / coordinate-transform /    */
/*  mosaicking programs.  Any number of input images can be merged into  */
/*  an output FITS file.  The attributes of the input are read from the  */
/*  input files; the attributes of the output are read a combination of  */
/*  the command line and a FITS header template file.                    */
/*                                                                       */
/*  This module, mArchiveGet, retrieve a single FITS image from          */
/*  a remote archive.  The supported data sets are retrieved through a   */
/*  basic URL GET.                                                       */
/*                                                                       */
/*************************************************************************/

int main(int argc, char **argv)
{
   int    socket, ihead, pastHeader;
   int    i, c, nread, count, port, raw;
   int    timeout;

   struct timeval timer;
 
   char  *dataref;

   char   buf     [MAXLEN];
   char   lead    [MAXLEN];
   char   head    [MAXLEN];
   char   request [MAXLEN];
   char   urlStr  [MAXLEN];
   char   hostStr [MAXLEN];
 
   char  *proxy;
   char   phostStr[MAXLEN];
   int    pport;
   char  *pdataref;
 
   int    fd;

   fd_set fdset;
 
   FILE  *fdebug;

   fdebug = stdout;

   fstatus = stdout;

   strcpy(hostStr, "irsa.ipac.caltech.edu");

   debug   =  0;
   opterr  =  0;
   port    = 80;
   raw     =  0;
   timeout =  0;
 
   while ((c = getopt(argc, argv, "drt:")) != EOF)
   {
      switch (c)
      {
	 case 'd':
	    debug = 1;
	    break;

	 case 'r':
	    raw = 1;
	    break;

	 case 't':
	    timeout = atoi(optarg);
	    break;

	 default:
	    printf("[struct stat=\"ERROR\", msg=\"Usage:  %s [-d][-r] remoteref localfile\"]\n",argv[0]);
	    exit(0);
	    break;
      }
   }

   if(argc - optind < 2)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage:  %s [-d][-r] remoteref localfile\"]\n",argv[0]);
      exit(0);
   }


   /* Try to open the output file */
 
   if(debug)
   {
      fprintf(fdebug, "DEBUG> localfile = [%s]\n", argv[optind+1]);
      fflush(fdebug);
   }

   fd = open(argv[optind+1], O_WRONLY | O_CREAT, 0644);
 
   if(fd < 0)
   {
      fprintf(fdebug, "[struct stat=\"ERROR\", msg=\"Output file(%s) open failed\"]\n", 
	 argv[optind+1]);
      exit(0);
   }
  
   /* Parse the reference string to get host and port info */

   strcpy(urlStr, argv[optind]);

   if(debug)
   {
      fprintf(fdebug, "DEBUG> urlStr  = [%s]\n", urlStr);
      fflush(fdebug);
   }

   parseUrl(urlStr, hostStr, &port, &dataref);

   if(*dataref == '\0')
   {
      printf("[struct stat=\"ERROR\", msg=\"No data reference given in URL\"]\n");
      exit(0);
   }

   proxy = getenv("http_proxy");
   
   if(proxy)
     parseUrl(proxy, phostStr, &pport, &pdataref);

   /* Connect to the port on the host we want */

   if(debug)
   {
      fprintf(fdebug, "DEBUG> hostStr = [%s]\n", hostStr);
      fprintf(fdebug, "DEBUG> port    =  %d\n",  port);
      fflush(fdebug);
   }
 
   if(proxy) {
     socket = tcp_connect(phostStr, pport);
   } else {
     socket = tcp_connect(hostStr, port);
   }
 
   /* Send a request for the file we want */
 
   if(raw) {
     if(proxy)
       sprintf(request, "GET %s/%s\r\n\r\n",
	       hostStr, dataref);
     else 
       sprintf(request, "GET %s\r\n\r\n",
	       dataref);       
   } else {
     if(proxy)
       sprintf(request, "GET %s HTTP/1.0\r\n\r\n",
	       urlStr);
     else 
       sprintf(request, "GET %s HTTP/1.0\r\nHost: %s\r\n\r\n",
	       dataref, hostStr);
   }
 
   if(debug)
   {
      fprintf(fdebug, "DEBUG> request = [%s]\n", request);
      fflush(fdebug);
   }

   send(socket, request, strlen(request), 0);
 

 
   /* Set a timeout in case the initial return takes forever */

   if(timeout > 0)
   {
      if(debug)
      {
	 fprintf(fdebug, "DEBUG> Setting timeout at %d seconds\n", timeout);
	 fflush(fdebug);
      }

      timer.tv_sec = (time_t)timeout;
      timer.tv_usec = 0;

      FD_ZERO(&fdset);
      FD_SET(socket, &fdset);

      if (select(FD_SETSIZE, &fdset, (fd_set *)0, (fd_set *)0, &timer) < 0)
      {
	 if (errno != EINTR) /* if not interrupt */
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Illegal return from select()\"]\n");
	    exit(0);
	 }
      }

      if(!FD_ISSET(socket, &fdset))
      {
	 printf("[struct stat=\"ERROR\", msg=\"Connection timed out\"]\n");
	 exit(0);
      }
   }

 
   /* Read the data coming back */
 
   count = 0;
   ihead = 0;

   pastHeader = 0;
 
   while(1)
   { 
      nread = read(socket, buf, MAXLEN);
 
      if(nread <= 0)
 	break;

      if(debug)
      {
	 fprintf(fdebug, "DEBUG> read %d bytes\n", nread);
	 fflush(fdebug);
      }
      
      if(!pastHeader && ihead == 0 && strncmp(buf, "H", 1) != 0)
      {
	 if(debug)
	 {
	    fprintf(fdebug, "DEBUG> No HTTP header on this one.\n");
	    fflush(fdebug);

	    for(i=0; i<40; ++i)
	      lead[i] = buf[i];

	    lead[40] = '\0';
	    fprintf(fdebug, "DEBUG> Starts with: [%s]... \n", lead);
	    fflush(fdebug);
	 }

	 pastHeader = 1;
      }

      if(!pastHeader)
      {
	 for(i=0; i<nread; ++i)
	 {
	    head[ihead] = buf[i];
	    ++ihead;
	 }

	 head[ihead] = '\0';

	 if(debug)
	 {
	    fprintf(fdebug, "DEBUG> Header ->\n%s\nDEBUG> Length = %d\n",
	       head, ihead);
	    fflush(fdebug);
	 }

	 for(i=0; i<ihead-3; ++i)
	 {
	    if(strncmp(head+i, "\r\n\r\n", 4) == 0 && ihead-i-4 > 0)
	    {
	       if(debug)
	       {
		  fprintf(fdebug, "DEBUG> End of header found: %d - %d\n",
		     i, i+3);

		  fprintf(fdebug, "DEBUG> Writing %d from header array\n",
		     ihead-i-4);

		  fflush(fdebug);
	       }

	       write(fd, head+i+4, ihead-i-4);

	       pastHeader = 1;

	       break;
	    }
	 }
      }
      else
      {
	 count += nread;

	 if(debug)
	 {
	    fprintf(fdebug, "DEBUG> Writing %d\n", nread);
	    fflush(fdebug);
	 }

	
	 write(fd, buf, nread);
      }
   }

   close(fd);

   checkHdr(argv[optind+1], 0, 0);
 
   printf("[struct stat=\"OK\", count=\"%d\"]\n", count);
   fflush(fdebug);
   exit(0);
}




/***********************************************/
/* This is the basic "make a connection" stuff */
/***********************************************/

int tcp_connect(char *hostname, int port)
{
   int                 sock_fd;
   struct hostent     *host;
   struct sockaddr_in  sin;


   if((host = gethostbyname(hostname)) == NULL) 
   {
      fprintf(stderr, "Couldn't find host %s\n", hostname);
      return(0);
   }

   if((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) 
   {
      fprintf(stderr, "Couldn't create socket()\n");
      return(0);
   }

   sin.sin_family = AF_INET;
   sin.sin_port = htons(port);
   bcopy(host->h_addr, &sin.sin_addr, host->h_length);

   if(connect(sock_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0)
   {
      fprintf(stderr, "%s: connect failed.\n", hostname);
      return(0);
   }

   return sock_fd;
}


int parseUrl(char *urlStr, char *hostStr, int *port, char **dataref) {
  
   char  *hostPtr;
   char  *portPtr;
   char   save;

   if(strncmp(urlStr, "http://", 7) != 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Invalid URL string (must start 'http://')\n"); 
      exit(0);
   }

   hostPtr = urlStr + 7;

   *dataref = hostPtr;

   while(1)
   {
      if(**dataref == ':' || **dataref == '/' || **dataref == '\0')
	 break;
      
      ++*dataref;
   }

   save = **dataref;

   **dataref = '\0';

   strcpy(hostStr, hostPtr);

   **dataref = save;


   if(**dataref == ':')
   {
      portPtr = *dataref+1;

      *dataref = portPtr;

      while(1)
      {
	 if(**dataref == '/' || **dataref == '\0')
	    break;
	 
	 ++*dataref;
      } 

      **dataref = '\0';

      *port = atoi(portPtr);

      **dataref = '/';

      if(*port <= 0)
      {
	 printf("[struct stat=\"ERROR\", msg=\"Illegal port number in URL\"]\n");
	 exit(0);
      }
   }
}
