/* Module: mGridExec.c 

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.1      John Good        26Nov04  Minor type changes for Solaris and
				   updated response structure
1.0      Gurmeet Singh    10Sep04  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>

#define MAXLEN          20000
#define MAX_RETRY_COUNT    10

char *url_encode (char *s);
int   tcp_connect(char *hostname, int port);

int debug = 0;


/************************************************/
/*                                              */
/* mGridExec -- Given the name of the           */
/* the zip file, it starts the execution of the */
/* mosaic on the grid                           */
/*                                              */
/************************************************/

int main(int argc, char **argv) 
{
   int   socket, port, rc, i, count;

   char  request[MAXLEN];
   char  server [256];
   char  base   [256];
   char  jobid  [256];
   char  line   [MAXLEN];
   char  message[MAXLEN];
   char  portStr[10];

   char *urlPtr, *u2, *u3, *u4;

   FILE *fout;

   long long size;

   struct stat file;


   /* Get the ZIP file name and check to see   */
   /* how big it is and whether we can read it */

   if(argc < 2) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-d] zipfile\"]\n",
	 argv[0]);
      fflush(stdout);
      exit(1);
   }

   if(argc > 2)
      debug = 1;

   if ((fout = fopen(argv[argc-1],"r")) == (FILE *)0) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Zip file %s not readable\"]\n",
	 argv[1]);
      fflush(stdout);
      exit(1);
   }

   if (stat(argv[1], &file)) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Size of %s is unknown\"]\n",
	 argv[1]);
      fflush(stdout);
      exit(1);
   }        

   size = file.st_size;


   /* Ask the Pegasus server where  */
   /* to find a mGridExec service   */

   strcpy(server, "pegasus.isi.edu");
   strcpy(base,   "/portal/mGridExec.html");

   port = 80;
   sprintf(request, "GET %s HTTP/1.0\r\nHOST: %s:%d\r\n\r\n",
      base, server, port);

   if(debug)
   {
      printf("DEBUG> request = [%s]\n", request);
      fflush(stdout);
   }



   /* Read the data coming back */

   urlPtr = u2 = u3 = u4 = NULL;

   count = 0;

   while(1)
   {
      socket = tcp_connect(server, port);

      send(socket, request, strlen(request), 0);

      for(i=0; i<MAXLEN; ++i)
	 line[i] = '\0';

      rc = recv(socket, line, MAXLEN, 0);

      if (rc <= 0) 
	 break;
      
      if (debug) 
      {
	 printf("DEBUG> response = [%s]\n", line);
	 fflush(stdout);
      }

      urlPtr = strstr(line,"url=http://");

      if ( urlPtr == NULL)  
      {
	 close(socket);

	 if (count < MAX_RETRY_COUNT) 
	 {
	    count++;
	    continue;
	 }
	 else 
	 {
	    printf("[struct stat=\"ERROR\", msg=\"Cannot determine mGridExec server address\"]\n");
	    fflush(stdout);
	    exit(1);
	 }
      }

      u2 = strstr(urlPtr + 11,":");

      strncpy(server, urlPtr+11,u2 - urlPtr - 11);

      server[u2-urlPtr-11] = '\0';

      u3 = strstr(u2+1,"/");

      strncpy(portStr, u2+1, u3 - u2 - 1);

      portStr[u3-u2-1] = '\0';

      port = atoi(portStr);

      u4 = strstr(u3,"\"");

      strncpy(base, u3, u4-u3);

      base[u4 - u3] = '\0';

      if (debug) 
	 printf("DEBUG> mGridExec server is at \"%s:%d%s\"\n",
	    server, port, base);
      
      break;
   }

   close(socket);


   /* Connect to the mGridExec Server */

   socket = tcp_connect(server, port);

   if (socket == 0) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Cannot connect to server\"]\n");
      fflush(stdout);
      exit(1);
   }


   /* Create and send the request (the ZIP file) */
   /* as a MIME multi-part message               */

   sprintf(request, "POST %s HTTP/1.0\r\n",base);
   send(socket, request, strlen(request), 0);

   sprintf(request,"Content-Type: multipart/form-data; boundary=---------------------------7d43e2b301fe\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"Host: 127.0.0.1\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"Content-Length: %lld\r\n",size + 459);
   send(socket, request, strlen(request), 0);

   sprintf(request,"\r\n-----------------------------7d43e2b301fe\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"Content-Disposition: form-data; name=\"proxyserver\"\r\n\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"birdie.isi.edu\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"-----------------------------7d43e2b301fe\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"Content-Disposition: form-data; name=\"filename\"; ");
   send(socket, request, strlen(request), 0);

   sprintf(request,"filename=\"out.zip\"\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"Content-Type: application/x-zip-compressed\r\n\r\n");
   send(socket, request, strlen(request), 0);

   while(1) 
   {
      rc = fread(line,1,MAXLEN,fout);

      if (rc <= 0) 
	 break;
      
      send(socket, line, rc, 0);
   }

   fclose(fout);

   sprintf(request,"\r\n-----------------------------7d43e2b301fe\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"Content-Disposition: form-data; name=\"B1\"\r\n\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"Submit\r\n");
   send(socket, request, strlen(request), 0);

   sprintf(request,"-----------------------------7d43e2b301fe--\r\n");
   send(socket, request, strlen(request), 0);

   for (i = 0; i < 1; i++) 
      send(socket, request, strlen(request), 0);


   /* Read the response (Job ID) */

   while(1) 
   {
      for(i=0; i<MAXLEN; ++i)
	 line[i] = '\0';

      rc = recv(socket, line, MAXLEN, 0);

      if (rc < 0) 
	 break;

      if (debug) 
      {
	 printf("DEBUG> response = [%s]\n", line);
	 fflush(stdout);
      }        

      urlPtr = strstr(line, "Job id ");

      if (urlPtr == NULL) 
      {
	 urlPtr = strstr(line,"Exception ");

	 if (urlPtr != NULL) 
	 {
	    u2 = strstr(urlPtr + 10,"#");

	    strncpy(message, urlPtr+10, u2 - urlPtr - 10);

	    message[u2-urlPtr-10] = '\0';

	    close(socket);
	    printf("[struct stat=\"ERROR\", msg=\"Exception: %s\"]\n",
	       message);
	    fflush(stdout);
	    exit(1);
	 }
	 else 
	 {
	    urlPtr = strstr(line,"</html>");

	    if (urlPtr != NULL) 
	    {
               close(socket);
	       printf("[struct stat=\"ERROR\", msg=\"Cannot determine request status\"]\n");
	       fflush(stdout);
	       exit(1);
	    }
	    else 
	       continue;
	 }
      }
      else 
      {
	 u2 = strstr(urlPtr + 7,".");

	 strncpy(jobid,urlPtr+7,u2 - urlPtr - 7);

	 jobid[u2-urlPtr-7] = '\0';

	 close(socket);
	 printf("[struct stat=\"OK\", jobid=%s]\n", jobid);
	 fflush(stdout);
	 exit(0);
      }

      break;
   }

   close(socket);
   printf("[struct stat=\"ERROR\", msg=\"Cannot determine request status\"]\n");
   fflush(stdout);
   exit(1);
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
