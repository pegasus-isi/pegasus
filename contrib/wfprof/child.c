#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "child.h"
#include "list.h"

static child_t *CHILDREN = NULL;

child_t *find_child(pid_t pid) {
  child_t *cur;
  LIST_FOREACH(CHILDREN, cur) {
    if (cur->pid == pid) return cur;
  }
  return NULL;
}

child_t *add_child(pid_t pid) {
  child_t *new;

  new = malloc(sizeof(child_t));
  new->pid = pid;
  new->ppid = 0;
  new->tgid = 0;
  new->insyscall = 0;
  new->sc_nr = -1;
  new->next = NULL;
  new->prev = NULL;

  LIST_APPEND(CHILDREN, new);
  
  return new;
}

void remove_child(pid_t pid) {
  child_t *del;
  del = find_child(pid);
  if (del == NULL)
    return;

  LIST_DELETE(CHILDREN, del);
  
  free(del);
}


int no_children() {
  return (CHILDREN == NULL) ? 1 : 0;
}


int read_exeinfo(child_t *c) {
  char link[128];
  int size;
  sprintf(link, "/proc/%d/exe", c->pid);
  size = readlink(link, c->exe, MAX_NAME);
  if (size >= 0 && size < MAX_NAME)
    c->exe[size] = '\0';
  return size;
}

int startswith(const char *line, const char *tok) {
  return strncmp(line, tok, strlen(tok)) == 0;
}

int read_meminfo(child_t *c) {
  char statf[128], line[BUFSIZ];
  FILE *f;

  sprintf(statf, "/proc/%d/status", c->pid);

  f = fopen(statf, "r");
  while (fgets(line, BUFSIZ, f) != NULL) {
    if (startswith(line, "PPid")) {
      sscanf(line,"PPid:%d\n",&(c->ppid));
    } else if (startswith(line, "Tgid")) {
      sscanf(line,"Tgid:%d\n",&(c->tgid));
    } else if (startswith(line,"VmPeak")) {
      sscanf(line,"VmPeak:%d kB\n",&(c->vmpeak));
    } else if (startswith(line,"VmHWM")) {
      sscanf(line,"VmHWM:%d kB\n",&(c->rsspeak));
    }
  }

  if (ferror(f)) {
    fclose(f);
    return -1;
  }

  return fclose(f);
}


int read_statinfo(child_t *c) {
  char statf[128];
  FILE *f;
  unsigned long utime, stime;
  long cutime, cstime;
  long clocks;

  sprintf(statf,"/proc/%d/stat", c->pid);

  f = fopen(statf,"r");

  fscanf(f, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu %ld %ld", &utime, &stime, &cutime, &cstime);

  /* Adjust by number of clock ticks per second */
  clocks = sysconf(_SC_CLK_TCK);
  c->utime = ((double)utime) / clocks;
  c->stime = ((double)stime) / clocks;
  c->cutime = ((double)cutime) / clocks;
  c->cstime = ((double)cstime) / clocks;

  if (ferror(f)) {
    fclose(f);
    return -1;
  }

  return fclose(f);
}

