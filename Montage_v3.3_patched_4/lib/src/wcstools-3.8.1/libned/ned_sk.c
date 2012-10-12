/***************************************************************************
/*  ned_sk.c  Xiuqin Wu  Nov. 9, 1993
/*
/* FUNCTIONS related to network:
/* ============================
/*
/* These should be only the functions that need to be modified if we ever
/* decide to use different interface for communication.  They will be
/* called by upper level functions (in ned_cli.c), but are not intended 
/* for the client programmers:
/* 
/* global variable:
/*    int ned_d;           /* the end point descriptor for communication */
/* 
/* 
/*    1. int connect_server(char *machine, char *service, int port);
/*       return a communication end point descriptor 
/* 
/*    2. disconnect_server()
/* 
/*    3. send_cmd(char *cmd)
/*       send a command to NED server
/* 
/*    4. ned_gets(char *string)
/*       get a string from NED server
/*    
/* 
/* FUNCTIONS specific to networking interface:
/* ==========================================
/* 
/* We decided to use connection-oriented communication(tcp protocol).
/* We provide four basic functions for connection, disconnection and transfer
/* of data. We transfer all the data in string to make it easy.
/* 
/* Functions using BSD Socket:
/* 
/*    1. int connectSock(char *machine, char *service, int port);
/*    2. disconnectSock();
/* 
/*    3. send_cmdSock(int s, char *cmd);
/*    4. get_stringSock(int s, char *string);
/* 
/***************************************************************************/

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#else
#include <sys/fcntl.h>
#endif

#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#include <sys/resource.h>

#include "nedc.h"
#include "ned_client.h"

#ifndef INADDR_NONE
#define INADDR_NONE       0xffffffff
#endif


int  ned_d;           /* end point descriptor for communication */
extern int ned_errno;


int
connect_server(machine, service, port)
char  *machine;
char  *service;
int   port;
{

   int  st;

   st  = connectSock(machine, service, port);
   if (st > 0) {
      ned_d = st;
      return(0);
      }
   else
      return(-1);
}

int
disconnect_server()
{
  disconnectSock();
  return(0);
}

int
send_cmd(cmd)
char  *cmd;
{
   int st;
   st = send_cmdSock(ned_d, cmd);
   return (st);
}

int
ned_gets(string)
char   *string;
{
   return(get_stringSock(ned_d, string));
}

/* below are closely network realted */


/*
 *  connectSock - allocate and connect a socket using TCP
 *  if can't get the service by name, use the port numebr directly
 */

int
connectSock(machine, service, port)
char   *machine;
char   *service;
int    port;
{
   struct hostent     *phe;
   struct servent     *pse;
   struct protoent    *ppe;
   struct sockaddr_in  sin;
   int                 s, type;     /* socket descriptor and socket type */

   memset((char *)&sin, 0, sizeof(struct sockaddr_in));
   sin.sin_family = AF_INET;

   /* map service name to port number */
   if (pse =getservbyname(service, "tcp"))
      sin.sin_port = pse->s_port;
   else if ( (sin.sin_port = htons((u_short)port)) == 0) {
      ned_errno = NE_SERVICE;
      return(-1);
      }

   /* map host name to IP addres, allowing for dotted decimal  */

   if (phe = gethostbyname(machine))
      memcpy((char *)&sin.sin_addr, phe->h_addr, phe->h_length);
   else  if ((sin.sin_addr.s_addr = inet_addr(machine)) == INADDR_NONE) {
      ned_errno = NE_HOST;
      return(-1);
      }

   /* map protocol name to protocol numebr */
   if ((ppe = getprotobyname("tcp")) == 0) {
      ned_errno = NE_PROTO;
      return(-1);
      }

   type = SOCK_STREAM;

   /* allocate socket */

   s = socket(PF_INET, type, ppe->p_proto);
   if (s < 0) {
      ned_errno = NE_SOCK;
      return(-1);
      }
   
   /* connect the socket */
   if (connect(s, (struct sockaddr *)&sin, sizeof(sin)) <0) {
      ned_errno = NE_CONNECT;
      return(-1);
      }

   return(s);
}

int
disconnectSock()
{
   int st;

   st = send_cmdSock(ned_d, "quit\n");
   close(ned_d);
   return(st);
}

int
send_cmdSock (sock, cmd)
int  sock;
char *cmd;
{
   int len ;

   len = write(sock, cmd, strlen(cmd));
   if (len < strlen(cmd))
      return(-1);
   else
      return(0);
}

char   inbuf[1024];
int    buf_index = 0;

int
read_inbuf(sock)
int	sock;
{
   int      n;
   int      nfds;
   fd_set   fdset;

   struct rlimit rlp;

#ifdef BSD
   nfds = getdtablesize();
#else
   if (getrlimit(RLIMIT_NOFILE, &rlp) == 0)
      nfds = rlp.rlim_cur;
   else {
      ned_errno = NE_TBLSIZE;  /* can't get table size */
      return(-1);
      }
#endif

   FD_ZERO(&fdset);
   FD_SET(sock, &fdset);

   if (select(nfds, &fdset, (fd_set *)0, (fd_set *)0,
	     (struct timeval *)0) < 0) {
      /*
      fprintf(stderr, "communiction is broken\n");
      exit(-1);
      */
      ned_errno = NE_BROKENC;
      return(-1);
      }
   if (FD_ISSET(sock, &fdset)) {
      memset((char *)inbuf, 0, sizeof(inbuf));
      n = read(sock, inbuf, sizeof(inbuf));
      if (n <= 0) {  /* the connection must be broken */
	 /*
	 fprintf(stderr, "communiction is broken\n");
	 exit(-1);
	 */
	 ned_errno = NE_BROKENC;
	 return(-1);
	 }
      buf_index = 0;
      }
   return(n);
}

/* return 0, all right; otherwise -1 */
int
get_stringSock(sock, string)
int   sock;
char  *string;
{
   static int n = 0;  /* number of chars left in inbuf */
   static int i = 0;  /* index for string */

   if (n == 0)
      if ((n = read_inbuf(sock)) < 0)  /* communication already broken */
	 return(-1);
   while (inbuf[buf_index] != '\n' && inbuf[buf_index] != '\r') {
      if (n > 0) {
	 string[i++] = inbuf[buf_index++];
	 n--;
	 }
      else
	 if ((n = read_inbuf(sock)) < 0)
	    return(-1);
      }
   if (inbuf[buf_index] == '\n' || inbuf[buf_index] == '\r') {
      string[i] = '\0';
      /* jump through the new lines, clean up last string input  */
      while (inbuf[buf_index] == '\n' || inbuf[buf_index] == '\r') {
	 if (n>0) {
	    buf_index++;
	    n--;
	    }
	 else
	    break;
	 }
      i = 0;
      return(0);
      }

   return(0);
}


