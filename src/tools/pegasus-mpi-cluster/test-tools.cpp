#include <string>
#include <unistd.h>
#include <sys/param.h>

#include "tools.h"
#include "failure.h"

using std::string;

void test_mkdirs() {
    if (mkdirs("test/scratch") < 0) {
        myfailures("mkdirs");
    }
    chdir("test/scratch");
    
    if (mkdirs("./foo") < 0) {
        myfailures("mkdirs");
    }
    if (mkdirs("../scratch/bar") < 0) {
        myfailures("mkdirs");
    }
    if (mkdirs("bar/../baz") < 0) {
        myfailures("mkdirs");
    }
    if (mkdirs(".boo") < 0) {
        myfailures("mkdirs");
    }
    
    char curdir[MAXPATHLEN];
    getcwd(curdir, MAXPATHLEN);
    
    char testdir[MAXPATHLEN];
    sprintf(testdir, "%s/bii", curdir);
    if (mkdirs(testdir) < 0) {
        myfailures("mkdirs");
    }
    
    sprintf(testdir, "%s/gii/gii", curdir);
    if (mkdirs(testdir) < 0) {
        myfailures("mkdirs");
    }
    
    chdir("..");
}

int main(int argc, char *argv[]) {
    get_host_memory();
    get_host_cpus();
    string host_name;
    get_host_name(host_name);
    test_mkdirs();
}
