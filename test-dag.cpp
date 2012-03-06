#include <string>
#include "stdio.h"

#include "stdlib.h"
#include "dag.h"
#include "failure.h"

void test_dag() {
    DAG dag("test/test.dag");
    
    Task *alpha = dag.get_task("Alpha");
    if (alpha == NULL) {
        failure("Didn't parse Alpha");
    }
    if (alpha->command.compare("/bin/echo Alpha") != 0) {
        failure("Command failed for Alpha: %s", alpha->command.c_str());
    }
    
    Task *beta = dag.get_task("Beta");
    if (beta == NULL) {
        failure("Didn't parse Beta");
    }
    if (beta->command.compare("/bin/echo Beta") != 0) {
        failure("Command failed for Beta: %s", beta->command.c_str());
    }
    
    if (alpha->children[0] != beta) {
        failure("No children");
    }
    
    if (beta->parents[0] != alpha) {
        failure("No parents");
    }
}

void test_rescue() {
    DAG dag("test/diamond.dag", "test/diamond.rescue");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (!a->success) {
        failure("A should have been successful");
    }

    if (!b->success) {
        failure("B should have been successful");
    }

    if (!c->success) {
        failure("C should have been successful");
    }

    if (d->success) {
        failure("D should have been failed");
    }
}

int main(int argc, char *argv[]) {
    test_dag();
    test_rescue();
    return 0;
}
