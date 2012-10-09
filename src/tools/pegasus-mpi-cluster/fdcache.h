#ifndef FDCACHE_H
#define FDCACHE_H

#include <string>
#include <map>
#include <cstdio>

using std::string;
using std::map;

class FDEntry {
public:
    string filename;
    FILE *file;
    FDEntry *prev;
    FDEntry *next;
    FDEntry(const string &filename, FILE *file);
    ~FDEntry();
};

class FDCache {
public:
    unsigned maxsize;
    unsigned hits;
    unsigned misses;
    
    FDEntry *first;
    FDEntry *last;
    map<string, FDEntry *> byname;
    
    FDCache(unsigned maxsize=0);
    ~FDCache();
    double hitrate();
    void access(FDEntry *entry);
    void push(FDEntry *entry);
    FDEntry *pop();
    FILE *open(string filename);
    int write(string filename, const char *data, int size);
    int size();
    void close();
};

#endif /* FDCACHE_H */
