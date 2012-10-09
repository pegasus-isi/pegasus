#include <cerrno>
#include <sys/resource.h>

#include "fdcache.h"
#include "log.h"
#include "failure.h"
#include "tools.h"

#define NOFILE_MAX 4096
#define NOFILE_RESERVE 64

FDEntry::FDEntry(const string &filename, FILE *file) {
    this->filename = filename;
    this->file = file;
    this->prev = NULL;
    this->next = NULL;
}

FDEntry::~FDEntry() {
    if (this->file != NULL) {
        fclose(this->file);
        this->file = NULL;
    }
}

FDCache::FDCache(unsigned maxsize) {
    this->maxsize = maxsize;
    this->first = NULL;
    this->last = NULL;
    this->hits = 0;
    this->misses = 0;
    
    // Determine the maximum number of open files allowed
    if (maxsize == 0) {
        int limit = 0;
        struct rlimit nofile;
        if (getrlimit(RLIMIT_NOFILE, &nofile)) {
            log_error("Unable to get NOFILE limit: %s", strerror(errno));
        } else {
            log_debug("Open files limit = %d (%d)", nofile.rlim_cur, nofile.rlim_max);
            limit = nofile.rlim_cur;
        }
        
        if (limit < 0) {
            // If there is no limit, then allow the max
            this->maxsize = NOFILE_MAX;
        } else if (limit == 0) {
            // If we couldn't find the limit, then the default is 64
            this->maxsize = 64;
        } else if (limit > NOFILE_MAX) {
            // No more than the max
            this->maxsize = NOFILE_MAX;
        } else {
            // In this case we reserve descriptors for other parts of the system
            // In the worst case we require at least 1 open descriptor
            this->maxsize = limit-NOFILE_RESERVE < 1 ? 1 : limit-NOFILE_RESERVE;
        } 
    }
    
    log_info("Setting max cached files = %u", this->maxsize);
}

FDCache::~FDCache() {
    this->close();
}

void FDCache::close() {
    FDEntry *i = first;
    while (i!=NULL) {
        FDEntry *next = i->next;
        delete i;
        i = next;
    }
    byname.clear();
    first = NULL;
    last = NULL;
}

int FDCache::size() {
    return this->byname.size();
}

double FDCache::hitrate() {
    double total = this->hits + this->misses;
    if (total == 0) {
        return 1.0;
    }
    return this->hits / total;
}

void FDCache::access(FDEntry *entry) {
    if (first == entry) {
        return;
    }
    
    // Make sure it is a valid request
    if (byname.size() == 0) {
        myfailure("Empty list");
    }
    if (entry == NULL) {
        myfailure("Invalid entry");
        return; /* Silence static analyzer */
    }
    if (entry->prev && entry->prev->next != entry) {
        myfailure("Entry not in list");
    }
    if (entry->next && entry->next->prev != entry) {
        myfailure("Entry not in list");
    }
    
    // If it is last, we need to update the last pointer
    if (last == entry) {
        last = entry->prev;
    }
    
    if (entry->prev) {
        entry->prev->next = entry->next;
    }
    if (entry->next) {
        entry->next->prev = entry->prev;
    }
    
    entry->prev = NULL;
    entry->next = first;
    first->prev = entry;
    first = entry;
}

void FDCache::push(FDEntry *entry) {
    // If there are too many descriptors in the cache,
    // then remove some
    while (this->byname.size() >= this->maxsize) {
        FDEntry *remove = this->pop();
        if (remove == NULL) {
            myfailure("Expected an entry");
        }
        delete remove;
    }
    
    if (last == NULL) {
        last = entry;
    }
    entry->next = first;
    entry->prev = NULL;
    if (first != NULL) {
        first->prev = entry;
    }
    first = entry;
    byname[entry->filename] = entry;
}

FDEntry *FDCache::pop() {
    if (last == NULL) {
        return NULL;
    }
    
    FDEntry *remove = last;
    
    byname.erase(last->filename);
    
    if (first == last) {
        // If it is the last one, then 
        // the list is empty
        first = NULL;
        last = NULL;
    } else {
        last = last->prev;
        last->next = NULL;
    }
    
    return remove;
}

FILE *FDCache::open(string filename) {
    // If the file is already in the cache, then
    // return it
    map<string, FDEntry *>::iterator i;
    i = byname.find(filename);
    if (i == byname.end()) {
        this->misses += 1;
    } else {
        this->hits += 1;
        FDEntry *entry = i->second;
        access(entry);
        return entry->file;
    }
    
    // Create directories as needed on file creation
    if (filename.find("/") != string::npos) {
        string path = filename.substr(0, filename.rfind("/"));
        if (mkdirs(path.c_str()) < 0) {
            log_error("Unable to create directory %s: %s", path.c_str(), 
                    strerror(errno));
            return NULL;
        }
    }
    
    // We always open the file for append because this may be one of many
    // records we need to write to the file
    FILE *file = fopen(filename.c_str(), "a");
    if (file == NULL) {
        return NULL;
    }
    
    FDEntry *entry = new FDEntry(filename, file);
    push(entry);
    
    return file;
}

int FDCache::write(string filename, const char *data, int size) {
    FILE *file = open(filename);
    if (file == NULL) {
        log_error("Error opening file %s: %s", filename.c_str(), 
                strerror(errno));
        return -1;
    }
    
    int rc = fwrite(data, 1, size, file);
    if (rc != size) {
        log_error("Error writing %d bytes to %s: %s", size, filename.c_str(), 
                strerror(errno));
        return -1;
    }
    if (fflush(file) != 0) {
        log_error("fflush failed on file %s: %s", filename.c_str(), 
                strerror(errno));
        return -1;
    }
#ifdef SYNC_IODATA
#ifdef DARWIN
    // OSX does not have fdatasync
    rc = fsync(fileno(file));
#else
    rc = fdatasync(fileno(file));
#endif
    if (rc != 0) {
        log_error("fsync/fdatasync failed on file %s: %s", filename.c_str(), 
                strerror(errno));
        return -1;
    }
#endif
    return 0;
}

