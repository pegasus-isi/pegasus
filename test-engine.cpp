#include <string>
#include <stdio.h>
#include <unistd.h>

#include "stdlib.h"
#include "dag.h"
#include "engine.h"
#include "failure.h"

void diamond_dag() {
    DAG dag("test/diamond.dag");
    Engine engine(dag);

    if (!engine.has_ready_task()) {
        myfailure("Did not queue root tasks");
    }
    
    Task *a = engine.next_ready_task();
    if (a->name.compare("A") != 0) {
        myfailure("Queued non root task %s", a->name.c_str());
    }
    
    if (engine.has_ready_task()) {
        myfailure("Queued non-root tasks");
    }
    
    engine.mark_task_finished(a, 0);
    
    if (!engine.has_ready_task()) {
        myfailure("Marking did not release tasks");
    }
    
    Task *bc = engine.next_ready_task();
    if (!engine.has_ready_task()) {
        myfailure("Marking did not release tasks");
    }
    
    Task *cb = engine.next_ready_task();
    if (engine.has_ready_task()) {
        myfailure("Marking released too many tasks");
    }
    
    if (bc->name.compare("B") != 0 && bc->name.compare("C") != 0) {
        myfailure("Wrong task released: %s", bc->name.c_str());
    }
    
    if (cb->name.compare("B") != 0 && cb->name.compare("C") != 0) {
        myfailure("Wrong task released: %s", cb->name.c_str());
    }
    
    engine.mark_task_finished(bc, 0);
    if (engine.has_ready_task()) {
        myfailure("Marking released a task when it shouldn't");
    }
    
    engine.mark_task_finished(cb, 0);
    if (!engine.has_ready_task()) {
        myfailure("Marking all parents did not release task D");
    }
    
    Task *d = engine.next_ready_task();
    if (d->name.compare("D") != 0) {
        myfailure("Not task D");
    }
    
    if (engine.has_ready_task()) {
        myfailure("No more tasks are available");
    }
    
    if (engine.is_finished()) {
        myfailure("DAG is not finished");
    }
    
    engine.mark_task_finished(d, 0);
    
    if (!engine.is_finished()) {
        myfailure("DAG is finished");
    }
}

void diamond_dag_rescue() {
    DAG dag("test/diamond.dag","test/diamond.rescue");
    Engine engine(dag);

    if (!engine.has_ready_task()) {
        myfailure("Should have ready D task");
    }
    
    Task *d = engine.next_ready_task();
    if (d->name.compare("D") != 0) {
        myfailure("Ready task is not D");
    }
    
    engine.mark_task_finished(d, 1);
    
    if (engine.has_ready_task()) {
        myfailure("Ready tasks even though D failed");
    }
    
    if (!engine.is_finished()) {
        myfailure("DAG should have been finished after D failed");
    }
    
    if (!engine.is_failed()) {
        myfailure("DAG should be failed");
    }
}

void diamond_dag_failure() {
    DAG dag("test/diamond.dag");
    Engine engine(dag);

    if (!engine.has_ready_task()) {
        myfailure("Did not queue root tasks");
    }
    
    Task *a = engine.next_ready_task();
    if (a->name.compare("A") != 0) {
        myfailure("Queued non root task %s", a->name.c_str());
    }
    
    if (engine.has_ready_task()) {
        myfailure("Queued non-root tasks");
    }
    
    engine.mark_task_finished(a, 1);
    
    if (engine.has_ready_task()) {
        myfailure("Released tasks even though parent failed");
    }
    
    if (!engine.is_finished()) {
        myfailure("DAG should have been finished after A failed");
    }
}


void read_file(char *fname, char *buf) {
    FILE *f = fopen(fname, "r");
    int read = fread(buf, 1, 1024, f);
    buf[read] = '\0';
    fclose(f);
}

void diamond_dag_newrescue() {
    char temp[1024];
    sprintf(temp,"file_XXXXXX");
    mkstemp(temp);
    
    DAG dag("test/diamond.dag");
    Engine engine(dag, temp);

    Task *a = engine.next_ready_task();
    engine.mark_task_finished(a, 0);
    
    Task *bc = engine.next_ready_task();
    engine.mark_task_finished(bc, 0);
    
    Task *cb = engine.next_ready_task();
    engine.mark_task_finished(cb, 0);
    
    Task *d = engine.next_ready_task();
    engine.mark_task_finished(d, 1);
    
    if (!engine.is_finished()) {
        myfailure("DAG should be finished");
    }
    
    if (!engine.is_failed()) {
        myfailure("DAG should be failed");
    }
    
    char buf[1024];
    read_file(temp, buf);
    if (strcmp(buf, "\nDONE A\nDONE B\nDONE C") != 0) {
        myfailure("Rescue file not updated properly: %s", temp);
    } else {
        unlink(temp);
    }
}

void diamond_dag_oldrescue() {
    char temp[1024];
    sprintf(temp, "file_XXXXXX");
    mkstemp(temp);
    
    DAG dag("test/diamond.dag", "test/diamond.rescue");
    Engine engine(dag, temp);

    if (!engine.has_ready_task()) {
        myfailure("Should have ready D task");
    }
    
    Task *d = engine.next_ready_task();
    if (d->name.compare("D") != 0) {
        myfailure("Ready task is not D");
    }
    
    engine.mark_task_finished(d, 0);
    
    if (engine.has_ready_task()) {
        myfailure("Ready tasks even though D finished");
    }
    
    if (!engine.is_finished()) {
        myfailure("DAG should have been finished after D finished");
    }
    
    if (engine.is_failed()) {
        myfailure("DAG should not be failed");
    }
    
    char buf[1024];
    read_file(temp, buf);
    if (strcmp(buf, "\nDONE A\nDONE B\nDONE C\nDONE D") != 0) {
        myfailure("Rescue file not updated properly: %s: %s", temp, buf);
    } else {
        unlink(temp);
    }
}

void diamond_dag_max_failures() {
    DAG dag("test/diamond.dag");
    Engine engine(dag, "", 1);

    if (!engine.has_ready_task()) {
        myfailure("Did not queue root tasks");
    }
    
    Task *a = engine.next_ready_task();
    if (a->name.compare("A") != 0) {
        myfailure("Queued non root task %s", a->name.c_str());
    }
    
    if (engine.has_ready_task()) {
        myfailure("Queued non-root tasks");
    }
    
    engine.mark_task_finished(a, 0);

    Task *bc = engine.next_ready_task();

    engine.mark_task_finished(bc, 1);

    if (engine.has_ready_task()) {
        myfailure("DAG should not have a ready task because %s failed", bc->name.c_str());
    }
}

void diamond_dag_retries() {
    int tries = 3;

    DAG dag("test/diamond.dag");
    Engine engine(dag, "", 0, tries);

    Task *a;
    
    for (int i=0; i<tries; i++) {
        if (!engine.has_ready_task()) {
            myfailure("A should have been ready");
        }

        a = engine.next_ready_task();
        if (a->name.compare("A") != 0) {
            myfailure("A should have been ready");
        }

        engine.mark_task_finished(a, 1);
    }
    
    if (engine.has_ready_task()) {
        myfailure("DAG should not have a ready task because A failed");
    }
}

void diamond_dag_retries2() {
    int tries = 3;

    DAG dag("test/diamond.dag");
    Engine engine(dag, "", 0, tries);

    Task *a;
    
    for (int i=0; i<tries-1; i++) {
        if (!engine.has_ready_task()) {
            myfailure("A should have been ready");
        }
        
        a = engine.next_ready_task();
        if (a->name.compare("A") != 0) {
            myfailure("A should have been ready");
        }
        
        engine.mark_task_finished(a, 1);
    }
    
    if (!engine.has_ready_task()) {
        myfailure("A should have been ready");
    }
    
    a = engine.next_ready_task();
    if (a->name.compare("A") != 0) {
        myfailure("A should have been ready");
    }
    
    engine.mark_task_finished(a, 0);

    if (!engine.has_ready_task()) {
        myfailure("DAG should have a ready task because A finally succeeded");
    }

    Task *bc = engine.next_ready_task();
    if (bc->name.compare("B")!=0 && bc->name.compare("C")!=0) {
        myfailure("B or C should have been ready");
    }
}

int main(int argc, char *argv[]) {
    diamond_dag();
    diamond_dag_failure();
    diamond_dag_max_failures();
    diamond_dag_retries();
    diamond_dag_retries2();
    diamond_dag_oldrescue();
    diamond_dag_newrescue();
    diamond_dag_rescue();
    return 0;
}
