#include "failure.h"
#include "master.h"
#include "dag.h"
#include "log.h"

void test_scheduler_124_8() {
    unsigned memory = 8192;
    unsigned threads = 8;
    unsigned cores = 4;
    unsigned sockets = 2;
    Host h("localhost", memory, threads, cores, sockets);

    DAG dag("test/PM953.dag");
    Task *one = dag.get_task("one");
    Task *two = dag.get_task("two");
    Task *four = dag.get_task("four");
    Task *eight = dag.get_task("eight");

    vector<unsigned> rone = h.allocate_resources(one);
    vector<unsigned> rtwo = h.allocate_resources(two);
    vector<unsigned> rfour = h.allocate_resources(four);

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

    vector<unsigned> reight = h.allocate_resources(eight);
    if (reight.size() != 8) {
        myfailure("task eight was bound to the wrong number of cores");
    }
}

void test_scheduler_44_2() {
    unsigned memory = 8192;
    unsigned threads = 8;
    unsigned cores = 4;
    unsigned sockets = 2;
    Host h("localhost", memory, threads, cores, sockets);

    DAG dag("test/PM953.dag");
    Task *four = dag.get_task("four");
    Task *four2 = dag.get_task("four2");

    vector<unsigned> rfour = h.allocate_resources(four);
    vector<unsigned> rfour2 = h.allocate_resources(four2);

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
    vector<unsigned> rtwo = h.allocate_resources(two);

    if (rtwo.size() != 2) {
        myfailure("task two was bound to the wrong number of cores");
    }

    if(rtwo[0] != 0 || rtwo[1] != 1) {
        myfailure("task two was bound to the wrong cores");
    }
}

int main(int argc, char **argv) {
    log_set_level(LOG_WARN);
    test_scheduler_124_8();
    test_scheduler_44_2();
    return 0;
}

