#include "master.h"
#include "failure.h"
#include "log.h"

using std::exception;

void test_new() {
    FDCache cache;
    cache.close();
}

void test_push() {
    FDCache cache(100);
    FDEntry *e1 = new FDEntry("foo", NULL);
    FDEntry *e2 = new FDEntry("bar", NULL);
    FDEntry *e3 = new FDEntry("baz", NULL);
    cache.push(e1);
    if (cache.first != e1 || cache.last != e1) {
        myfailure("e1 insert failed");
    }
    cache.push(e2);
    if (cache.first != e2 || cache.last != e1) {
        myfailure("e2 insert failed");
    }
    cache.push(e3);
    if (cache.first != e3 || cache.last != e1) {
        myfailure("e3 insert failed");
    }
    if (cache.size() != 3) {
        myfailure("inserts did not work");
    }
    if (cache.first != e3) {
        myfailure("e3 should be first");
    }
    if (cache.last != e1) {
        myfailure("e1 should be last");
    }
    if (cache.first->next != e2) {
        myfailure("e2 should be after first");
    }
    if (cache.last->prev != e2) {
        myfailure("e2 should be before last");
    }
    if (e2->prev != cache.first) {
        myfailure("first should be before e2");
    }
    if (e2->next != cache.last) {
        myfailure("last should be after e2");
    }
    cache.close();
}

void test_pop() {
    FDCache cache(100);
    FDEntry *e1 = new FDEntry("foo", NULL);
    FDEntry *e2 = new FDEntry("bar", NULL);
    FDEntry *e3 = new FDEntry("baz", NULL);
    cache.push(e1);
    cache.push(e2);
    cache.push(e3);
    
    FDEntry *pe1 = cache.pop();
    if (cache.size() != 2) {
        myfailure("cache size should be 2");
    }
    if (pe1 != e1) {
        myfailure("Pop of e1 failed");
    }
    if (cache.first != e3) {
        myfailure("e3 should be first");
    }
    if (cache.last != e2) {
        myfailure("e2 should be last");
    }
    if (e3->next != e2) {
        myfailure("e3->next should be e2");
    }
    if (e2->prev != e3) {
        myfailure("e2->prev should be e3");
    }
    
    FDEntry *pe2 = cache.pop();
    if (cache.size() != 1) {
        myfailure("size should be 1");
    }
    if (pe2 != e2) {
        myfailure("Pop of e2 failed");
    }
    if (cache.first != e3) {
        myfailure("e3 should be first");
    }
    if (cache.last != e3) {
        myfailure("e3 should be last");
    }
    if (e3->prev != NULL) {
        myfailure("e3->prev should be NULL");
    }
    if (e3->next != NULL) {
        myfailure("e3->next should be NULL");
    }

    FDEntry *pe3 = cache.pop();
    if (cache.size() != 0) {
        myfailure("Cache should be empty");
    }
    if (pe3 != e3) {
        myfailure("Pop of e3 failed");
    }
    if (cache.first != NULL) {
        myfailure("first should be NULL");
    }
    if (cache.last != NULL) {
        myfailure("last should be NULL");
    }
    
    FDEntry *null = cache.pop();
    if (null != NULL) {
        myfailure("Pop of empty list failed");
    }
    
    delete e1;
    delete e2;
    delete e3;
    
    cache.close();
}

void test_limit() {
    FDCache cache(2);
    FDEntry *e1 = new FDEntry("foo", NULL);
    FDEntry *e2 = new FDEntry("bar", NULL);
    FDEntry *e3 = new FDEntry("baz", NULL);
    cache.push(e1);
    cache.push(e2);
    cache.push(e3);
    if (cache.first != e3) {
        myfailure("first should be e3");
    }
    if (cache.last != e2) {
        myfailure("last should be e2");
    }
    if (e3->prev != NULL || e3->next != e2) {
        myfailure("e3->prev should be null, e3->next should be e2");
    }
    if (e2->prev != e3 || e2->next != NULL) {
        myfailure("e2->prev should be e2, e2->next should be NULL");
    }
    cache.close();
}

void test_access() {
    FDCache cache;
    FDEntry *e1 = new FDEntry("foo", NULL);
    FDEntry *e2 = new FDEntry("bar", NULL);
    FDEntry *e3 = new FDEntry("baz", NULL);
    cache.push(e1);
    cache.push(e2);
    cache.push(e3);
    // Order should now be e3->e2->e1
    
    cache.access(e1);
    // Order should now be e1->e3->e2
    if (cache.first != e1 || 
        e1->next != e3 || e1->prev != NULL ||
        e3->next != e2 || e3->prev != e1 ||
        e2->next != NULL || e2->prev != e3 ||
        cache.last != e2) {
        myfailure("access e1 failed");
    }
    
    cache.access(e3);
    // Order should now be e3->e1->e2
    if (cache.first != e3 || 
        e3->next != e1 || e3->prev != NULL ||
        e1->next != e2 || e1->prev != e3 ||
        e2->next != NULL || e2->prev != e1 ||
        cache.last != e2) {
        myfailure("access e3 failed");
    }
    
    cache.access(e3);
    // Order should remain the same
    if (cache.first != e3 || cache.last != e2 ||
        e3->next != e1 || e3->prev != NULL ||
        e1->next != e2 || e1->prev != e3 ||
        e2->next != NULL || e2->prev != e1) {
        myfailure("access e3 #2 failed");
    }
    
    if (cache.pop() != e2) {
        myfailure("pop should return e2");
    }
    
    // Order should be e3->e1
    if (cache.first != e3 || cache.last != e1 ||
        e3->prev != NULL || e3->next != e1 ||
        e1->prev != e3 || e1->next != NULL) {
        myfailure("pop failed");
    }
    
    cache.access(e1);
    //Order should be e1->e3 now
    if (cache.first != e1 || cache.last != e3 ||
        e1->prev != NULL || e1->next != e3 ||
        e3->prev != e1 || e3->next != NULL) {
        myfailure("access e1 #2 failed");
    }
    
    delete e2;
    
    cache.close();
}

void test_open() {
    FDCache cache;
    FILE *f = cache.open("test/scratch/fdcache.dat");
    if (f == NULL) {
        myfailure("Open failed");
    }
    if (cache.misses != 1 || cache.hits != 0) {
        myfailure("should have one miss and no hits");
    }
    FILE *g = cache.open("test/scratch/fdcache.dat");
    if (f != g) {
        myfailure("caching failed");
    }
    if (cache.misses != 1 || cache.hits != 1) {
        myfailure("should have one hit and one miss");
    }
    cache.close();
}

void test_write() {
    FDCache cache;
    char message[] = "test write\n";
    cache.write("test/scratch/test_write", message, strlen(message));
    cache.close();
}

int main(int argc, char **argv) {
    try {
        log_set_level(LOG_ERROR);
        log_trace("test_new");
        test_new();
        log_trace("test_push");
        test_push();
        log_trace("test_pop");
        test_pop();
        log_trace("test_limit");
        test_limit();
        log_trace("test_access");
        test_access();
        log_trace("test_open");
        test_open();
        log_trace("test_write");
        test_write();
        return 0;
    } catch (exception &error) {
        log_error("ERROR: %s", error.what());
        return 1;
    }
}

