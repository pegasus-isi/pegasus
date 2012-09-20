#include <string>
#include <unistd.h>
#include <sys/param.h>
#include <assert.h>

#include "tools.h"

void test_is_executable() {
    assert(is_executable("./test-tools"));
    assert(!is_executable("./notfound"));
}

void test_pathfind() {
    assert(pathfind("sh") == "/bin/sh");
    assert(pathfind("echo") == "/bin/echo");
    assert(pathfind("/notfound") == "/notfound");
    assert(pathfind("./notfound") == "./notfound");
    assert(pathfind("test/notfound") == "test/notfound");
}

void test_mkdirs() {
    assert(mkdirs("test/scratch") >= 0);
    chdir("test/scratch");
    
    assert(mkdirs("./foo") >= 0);
    assert(mkdirs("../scratch/bar") >= 0);
    assert(mkdirs("bar/../baz") >= 0);
    assert(mkdirs(".boo") >= 0);
    
    char temp[PATH_MAX];
    string curdir = getcwd(temp, PATH_MAX);
    
    string testdir = curdir + "/bii";
    assert(mkdirs(testdir.c_str()) >= 0);
    
    testdir = curdir + "/gii/gii";
    assert(mkdirs(testdir.c_str()) >= 0);
    
    chdir("../..");
}

int main(int argc, char *argv[]) {
    get_host_memory();
    get_host_cpus();
    string host_name;
    get_host_name(host_name);
    test_mkdirs();
    test_is_executable();
    test_pathfind();
}
