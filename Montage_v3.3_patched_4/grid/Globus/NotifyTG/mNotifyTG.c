/* Module: mNotify.c

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      John Good        11Nov05  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#define MAXLEN 20000

char *url_encode (char *s);
int   tcp_connect(char *hostname, int port);

int debug = 0;


/********************************************/
/*                                          */
/* mNotifyTG -- Contact (via URL) a job     */
/* the job submission machine with messages */
/* about job execution and completion.      */
/*                                          */
/********************************************/

int main(int argc, char **argv)
{
   int    i, j, rc, socket, port, count;
  
   char   line      [MAXLEN];
   char   request   [MAXLEN];
   char   base      [MAXLEN];
   char   constraint[MAXLEN];
   char   server    [MAXLEN];
   char   result    [MAXLEN];

   char  *directory;
   char  *msg;


   /* Get the log location and message */

   if(argc < 3)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s directory message\"]\n", argv[0]);
      exit(0);
   }

   strcpy(server, "montage-lx.ipac.caltech.edu");

   port = 8001;

   strcpy(base, "/cgi-bin/Notify/nph-notify?");

   directory = url_encode(argv[1]);
   msg       = url_encode(argv[2]);

   sprintf(constraint, "directory=%s&msg=%s", directory, msg);


   /* Connect to the port on the host we want */

   socket = tcp_connect(server, port);
  

   /* Send a request for the file we want */

   sprintf(request, "GET %s%s HTTP/1.0\r\nHOST: %s:%d\r\n\r\n",
      base, constraint, server, port);

   if(debug)
   {
      printf("DEBUG> request = [%s]\n", request);
      fflush(stdout);
   }

   send(socket, request, strlen(request), 0);


   /* Read the data coming back */

   count = 0;
   j     = 0;

   while(1)
   {
      for(i=0; i<MAXLEN; ++i)
         line[i] = '\0';

      rc = recv(socket, line, 1, 0);

      if(rc <= 0)
         break;

      for(i=0; i<rc; ++i)
      {
	 result[j] = line[i];
	 ++j;
      }

      count += rc;
   }

   result[j] = '\0';

   for(i=0; i<strlen(result); ++i)
   {
      if(result[i] == '\t'
      || result[i] == '\r'
      || result[i] == '\n')
	 result[i] =  ' ';
   }

   if(count == 0)
      printf("[struct stat=\"ERROR\", msg=\"No return message.\"]\n");

   else if(strncmp(result, "ERROR: ", 7) == 0)
      printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", result+7);
   
   else
      printf("[struct stat=\"OK\", msg=\"%s\"]\n", result);

   fflush(stdout);

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
      printf("[struct stat=\"ERROR\", msg=\"Couldn't find host %s\"]\n", hostname);
      fflush(stdout);
      return(0);
   }

   if((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Couldn't create socket()\"]\n");
      fflush(stdout);
      return(0);
   }

   sin.sin_family = AF_INET;
   sin.sin_port = htons(port);
   bcopy(host->h_addr, &sin.sin_addr, host->h_length);

   if(connect(sock_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"%s: connect failed.\"]\n", hostname);
      fflush(stdout);
      return(0);
   }

   return sock_fd;
}



/**************************************/
/* This routine URL-encodes a string  */
/**************************************/

static unsigned char hexchars[] = "0123456789ABCDEF";

char *url_encode(char *s)
{
   int      len;
   register int i, j;
   unsigned char *str;

   len = strlen(s);

   str = (unsigned char *) malloc(3 * strlen(s) + 1);

   j = 0;

   for (i=0; i<len; ++i)
   {
      str[j] = (unsigned char) s[i];

      if (str[j] == ' ')
      {
         str[j] = '+';
      }
      else if ((str[j] < '0' && str[j] != '-' && str[j] != '.') ||
               (str[j] < 'A' && str[j] > '9')                   ||
               (str[j] > 'Z' && str[j] < 'a' && str[j] != '_')  ||
               (str[j] > 'z'))
      {
         str[j++] = '%';

         str[j++] = hexchars[(unsigned char) s[i] >> 4];

         str[j]   = hexchars[(unsigned char) s[i] & 15];
      }

      ++j;
   }

   str[j] = '\0';

   return ((char *) str);
}
