#include "failure.h"
#include "master.h"
#include "dag.h"
#include "log.h"

void test_scheduler_124_8() {
    unsigned memory = 8192;
    cpu_t threads = 8;
    cpu_t cores = 4;
    cpu_t sockets = 2;
    Host h("localhost", memory, threads, cores, sockets);

    DAG dag("test/PM953.dag");
    Task *one = dag.get_task("one");
    Task *two = dag.get_task("two");
    Task *four = dag.get_task("four");
    Task *eight = dag.get_task("eight");

    vector<cpu_t> rone = h.allocate_resources(one);
    vector<cpu_t> rtwo = h.allocate_resources(two);
    vector<cpu_t> rfour = h.allocate_resources(four);

    if (rone.size() != 0) {
        myfailure("task one was bound to a core");
    }
    if (rtwo.size() != 2) {
        myfailure("task two was bound to the wrong number of cores");
    }
    if (rfour.size() != 4) {
        myfailure("task four was bound to the wrong number of cores");
    }

    if (rtwo[0] != 0 || rtwo[1] != 1) {
        myfailure("task two was bound to the wrong cores");
    }
    if (rfour[0] != 4 || rfour[1] != 5 || rfour[2] != 6 || rfour[3] != 7) {
        myfailure("task four was bound to the wrong cores");
    }

    h.release_resources(one);
    h.release_resources(two);
    h.release_resources(four);

    vector<cpu_t> reight = h.allocate_resources(eight);
    if (reight.size() != 8) {
        myfailure("task eight was bound to the wrong number of cores");
    }
}

void test_scheduler_44_2() {
    unsigned memory = 8192;
    cpu_t threads = 8;
    cpu_t cores = 4;
    cpu_t sockets = 2;
    Host h("localhost", memory, threads, cores, sockets);

    DAG dag("test/PM953.dag");
    Task *four = dag.get_task("four");
    Task *four2 = dag.get_task("four2");

    vector<cpu_t> rfour = h.allocate_resources(four);
    vector<cpu_t> rfour2 = h.allocate_resources(four2);

    if (rfour.size() != 4) {
        myfailure("task four was bound to the wrong number of cores");
    }
    if (rfour2.size() != 4) {
        myfailure("task four2 was bound to the wrong number of cores");
    }

    if (rfour[0] != 0 || rfour[1] != 1 || rfour[2] != 2 || rfour[3] != 3) {
        myfailure("task four was bound to the wrong cores");
    }
    if (rfour2[0] != 4 || rfour2[1] != 5 || rfour2[2] != 6 || rfour2[3] != 7) {
        myfailure("task four2 was bound to the wrong cores");
    }

    h.release_resources(four);

    Task *two = dag.get_task("two");
    vector<cpu_t> rtwo = h.allocate_resources(two);

    if (rtwo.size() != 2) {
        myfailure("task two was bound to the wrong number of cores");
    }

    if(rtwo[0] != 0 || rtwo[1] != 1) {
        myfailure("task two was bound to the wrong cores");
    }
}

void test_scheduler_2222_4() {
    unsigned memory = 8192;
    cpu_t threads = 8;
    cpu_t cores = 4;
    cpu_t sockets = 2;
    Host h("localhost", memory, threads, cores, sockets);

    DAG dag("test/PM953.dag");
    Task *two = dag.get_task("two");
    Task *two2 = dag.get_task("two2");
    Task *two3 = dag.get_task("two3");
    Task *two4 = dag.get_task("two4");

    vector<cpu_t> rtwo = h.allocate_resources(two);
    vector<cpu_t> rtwo2 = h.allocate_resources(two2);
    vector<cpu_t> rtwo3 = h.allocate_resources(two3);
    vector<cpu_t> rtwo4 = h.allocate_resources(two4);

    if (rtwo.size() != 2 || rtwo[0] != 0 || rtwo[1] != 1) {
        myfailure("task two was bound to the wrong cores");
    }
    if (rtwo2.size() != 2 || rtwo2[0] != 2 || rtwo2[1] != 3) {
        myfailure("task two2 was bound to the wrong cores");
    }
    if (rtwo3.size() != 2 || rtwo3[0] != 4 || rtwo3[1] != 5) {
        myfailure("task two3 was bound to the wrong cores");
    }
    if (rtwo4.size() != 2 || rtwo4[0] != 6 || rtwo4[1] != 7) {
        myfailure("task two4 was bound to the wrong cores");
    }

    // Clear up cores 2 and 3
    h.release_resources(two2);
    h.release_resources(two3);

    // Task four should not be bound to any cores
    Task *four = dag.get_task("four");
    vector<cpu_t> rfour = h.allocate_resources(four);

    if (rfour.size() != 0) {
        myfailure("task four was bound to fragmented cores");
    }
}

int main(int argc, char **argv) {
    log_set_level(LOG_WARN);
    test_scheduler_124_8();
    test_scheduler_44_2();
    test_scheduler_2222_4();
    return 0;
}

