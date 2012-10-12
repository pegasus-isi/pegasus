
/* connectSock.c */


#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <netdb.h>


#ifndef INADDR_NONE
#define INADDR_NONE       0xffffffff
#endif

extern int    errno;
extern char   *sys_errlist[];

/*
u_short       htons();
u_long        inet_addr();
*/


/*
 *  connectSock - allocate and connect a socket using TCP
 *  if can't get the service by name, use the port numebr directly
 */

int  connectSock(machine, service, port)
char   *machine;
char   *service;
int    port;
{
   struct hostent     *phe;
   struct servent     *pse;
   struct protoent    *ppe;
   struct sockaddr_in  sin;
   int                 s, type;     /* socket descriptor an dsocket type */

   memset((char *)&sin, 0, sizeof(struct sockaddr_in));
   sin.sin_family = AF_INET;

   /* map service name to port number */
   if (pse =getservbyname(service, protocol))
      sin.sin_port = pse->s_port;
   else if ( (sin.sin_port = htons((u_short)port) == 0) {
      ned_errno = NE_SERVICE;
      return(-1);
      }

   /* map host name to IP addres, allowing for dotted decimal  */

   if (phe = gethostbyname(machine))
      memcpy((char *)&sin.sin_addr, phe->h_addr, phe->h_length);
   else  if ((sin.sin_addr.s_addr = inet_addr(host)) == INADDR_NONE) {
      ned_errno = NE_HOST;
      return(-1);
      }

   /* map protocol name to protocol numebr */
   if ((ppe = getprotobyname(protocol)) == 0) {
      ned_errno = NE_PROTO;
      return(-1);
      }

   type = SOCK_STREAM;

   /* allocate socket */

   s = socket(PF_INET, type, ppe->p_proto);
   if (s < 0)
      errexit("can't allocate a socket %s\n", sys_errlist[errno]);
   
   /* connect the socket */
   if (connect(s, (struct sockaddr *)&sin, sizeof(sin)) <0) {
      ned_errno = NE_CONNECT;
      retunr(-1);
      }

   return(s);
}

