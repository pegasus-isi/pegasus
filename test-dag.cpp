#include <string>
#include <stdio.h>

#include "stdlib.h"
#include "dag.h"
#include "failure.h"

void test_dag() {
    DAG dag("test/test.dag");
    
    Task *alpha = dag.get_task("Alpha");
    if (alpha == NULL) {
        myfailure("Didn't parse Alpha");
    }
    if (alpha->command.compare("/bin/echo Alpha") != 0) {
        myfailure("Command failed for Alpha: %s", alpha->command.c_str());
    }
    
    Task *beta = dag.get_task("Beta");
    if (beta == NULL) {
        myfailure("Didn't parse Beta");
    }
    if (beta->command.compare("/bin/echo Beta") != 0) {
        myfailure("Command failed for Beta: %s", beta->command.c_str());
    }
    
    if (alpha->children[0] != beta) {
        myfailure("No children");
    }
    
    if (beta->parents[0] != alpha) {
        myfailure("No parents");
    }
}

void test_rescue() {
    DAG dag("test/diamond.dag", "test/diamond.rescue");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (!a->success) {
        myfailure("A should have been successful");
    }

    if (!b->success) {
        myfailure("B should have been successful");
    }

    if (!c->success) {
        myfailure("C should have been successful");
    }

    if (d->success) {
        myfailure("D should have been failed");
    }
}

void test_pegasus_dag() {
    DAG dag("test/pegasus.dag");
    
    Task *a = dag.get_task("A");
    
    if (a->pegasus_id.compare("1") != 0) {
        myfailure("A should have had pegasus_id");
    }
    
    if (a->pegasus_transformation.compare("mDiffFit:3.3") != 0) {
        myfailure("A should have had pegasus_transformation");
    }
    
    Task *b = dag.get_task("B");
    
    if (b->pegasus_id.compare("2") != 0) {
        myfailure("B should have had pegasus_id");
    }
    
    if (b->pegasus_transformation.compare("mDiff:3.3") != 0) {
        myfailure("B should have had pegasus_transformation");
    }
}

void test_memory_dag() {
    DAG dag("test/memory.dag");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (a->memory != 0) {
        myfailure("A should require 0 MB memory");
    }
    
    if (b->memory != 100) {
        myfailure("B should require 100 MB memory");
    }
    
    if (c->memory != 100) {
        myfailure("C should require 100 MB memory");
    }
    
    if (d->memory != 100) {
        myfailure("D should require 100 MB memory");
    }
}

void test_cpu_dag() {
    DAG dag("test/cpus.dag");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (a->cpus != 0) {
        myfailure("A should require 0 CPUs");
    }
    
    if (b->cpus != 2) {
        myfailure("B should require 2 CPUs");
    }
    
    if (c->cpus != 2) {
        myfailure("C should require 2 CPUs");
    }
    if (c->memory != 100) {
        myfailure("C should require 100 MB memory");
    }
    
    if (d->cpus != 2) {
        myfailure("D should require 2 CPUs");
    }
}

void test_tries_dag() {
    DAG dag("test/tries.dag", "", true, 3);
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (a->tries != 2) {
        myfailure("A should have 2 tries");
    }
    
    if (b->tries != 5) {
        myfailure("B should have 5 tries");
    }
    
    if (c->tries != 3) {
        myfailure("C should have 3 tries");
    }
    
    if (d->tries != 2) {
        myfailure("D should have 2 tries");
    }
    if (d->memory != 100) {
        myfailure("D should require 100 MB memory");
    }
}

void test_priority_dag() {
    DAG dag("test/priority.dag");
    
    Task *g = dag.get_task("G");
    Task *i = dag.get_task("I");
    Task *d = dag.get_task("D");
    Task *e = dag.get_task("E");
    Task *o = dag.get_task("O");
    Task *n = dag.get_task("N");
    
    if (g->priority != 10) {
        myfailure("G should have priority 10");
    }
    
    if (i->priority != 9) {
        myfailure("I should have priority 9");
    }
    
    if (d->priority != 8) {
        myfailure("D should have priority 8");
    }
    
    if (e->priority != 7) {
        myfailure("E should have priority 7");
    }
    
    if (o->priority != -4) {
        myfailure("O should have priority -4");
    }
    
    if (n->priority != -5) {
        myfailure("N should have priority -5");
    }
}

int main(int argc, char *argv[]) {
    test_dag();
    test_rescue();
    test_pegasus_dag();
    test_memory_dag();
    test_cpu_dag();
    test_tries_dag();
    test_priority_dag();
    return 0;
}
