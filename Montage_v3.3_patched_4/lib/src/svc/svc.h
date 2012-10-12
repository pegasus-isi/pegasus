/**
    \file       svc.h
    \author     <a href="mailto:jcg@ipac.caltech.edu">John Good</a>
 */

/**
    \mainpage   libsvc: Service I/O and Return Structure Handling Library
    \htmlinclude docs/svc.html
 */

/**
    \page       Structure Format
    \htmlinclude docs/struct.html
 */

#ifndef ISIS_SVC
#define ISIS_SVC

#define SVC_OK     0
#define SVC_ERROR -1

#define SVC_MAXSVC    32
#define SVC_STRLEN  4096
#define SVC_SVCCNT   128

typedef struct
{
   int  nalloc;
   int  count;
   char **key;
   char **val;
}
   SVC;

void set_apputil_debug(int flag);
void svc_debug(FILE *stream);
void svc_check();
int svc_init(char *svcstr);
int svc_register(int index, char *name, char *sig, char *quit);
int svc_close(int index);
int svc_closeall();
int svc_run(char * svcstr);
int svc_send(int index, char *cmd);
char *svc_receive(int index);
char *svc_fgets(int index);
int svc_command(int svc, char *cmdstr);
char *svc_value(char *ref);
int svc_getargs (char *cmd, char **cmdv);
void svc_sigset();
void svc_sighandler();
SVC *svc_struct(char *instr);
char *svc_stripblanks(char *ptr, int len, int quotes);
int svc_free(SVC *svc);
char *svc_val(char *structstr, char *key, char *val);

#endif /* ISIS_SVC */
