"""
filelock.py: Provides NFS-safe locking around a DB File.
"""
from __future__ import print_function

##
#  Copyright 2007-2010 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Revision : $Revision: 2012 $

import os.path
import time
import random
import anydbm
import atexit
import logging

# Keep list of files to delete
at_exit = {}

def intent_exit_handler():
    # Cleanup keys when exiting
    for key in at_exit:
        try:
            logger.debug("unlinking %s" % (key))
            os.unlink(key)
        except:
            logger.debug("error unlinking %s" % (key))

logger = logging.getLogger(__name__)

class Intent:
    """
    The Intent class coordinates intentions between multiple
    concurrent site-selector instances. For this reason, it provides
    access to a DB file to record arbitrary scalar intentions
    into. The file is locked using NFS-safe (so is hoped) file
    locks.
    """

    def __init__(self):
        self.m_filename = None
        self.m_pid = os.getpid()
        self.m_count = {}

    def new(self, fn):
        """
        This function records the filename as the database file to
        either create or to connect to. If the file does not exist
        yet, it will not be created in the initialization
        function. However, some simple checks are employed to see, if
        the file will be creatable and/or writable, should it not
        exist.
        """
        # purpose: Initialize class
        # paramtr: $fn (IN): path to intent database file

        if os.path.isfile(fn):
            # File already exists
            if os.access(fn, os.R_OK) and os.access(fn, os.W_OK):
                # Good!
                self.m_filename = fn
                return True
            else:
                # Cannot read/write to file
                return False
        else:
            # File does not exist yet
            dir = os.path.dirname(fn)
            
            # Check if directory exists and is writable
            if os.path.exists(dir) and os.access(dir, os.W_OK):
                # Everything looks good!
                self.m_filename = fn
                return True

        # Failed
        return False

    def create_lockfile(self, fn):
        # Create a lock file NFS-reliably
        # warning: use create_lock, not this function
        # paramtr: $fn (IN): name of main file to lock
        # returns: 1 on success, 0 on failure to lock
        tolock = fn
        lock = "%s.lock" % (tolock)
        uniq = "%s.%d" % (tolock, self.m_pid)

        if os.path.isfile(uniq):
            logger.warn("Locking: open %s: file already exists" % (uniq))
            # os.unlink(uniq)
            return False

        if os.path.isfile(lock):
            logger.warn("Locking: open %s: file already exists" % (lock))
            # os.unlink(lock)
            return False

        try:
            my_lock = open(uniq, "w")
        except:
            logger.warn("Locking: open %s: creating file" % (uniq))
            return False
        else:
            at_exit[uniq] = 1
            my_lock.write("%d\n" % (self.m_pid))
            my_lock.close()

        try:
            os.link(uniq, lock)
        except:
            # Unable to create link, check error
            logger.warn("while locking %s" % (uniq))
            try:
                stats = os.stat(uniq)
            except:
                # Error, no need to do anything
                logger.warn("error trying to stat %s" % uniq)
                pass
            else:
                if stats.st_nlink == 2:
                    # Lock successful
                    logger.info("link-count locked")
                    at_exit[lock] = 1
                    os.unlink(uniq)
                    at_exit.pop(uniq)
                    return True
        else:
            # Created link
            logger.info("hardlinnk locked")
            at_exit[lock] = 1
            os.unlink(uniq)
            at_exit.pop(uniq)
            return True
        
        return False

    def break_lock(self, fn):
        # purpose: check for a dead lock file, and remove if dead
        # paramtr: $fn (IN): name of the file to create lock file for
        # returns: None if the lock is valid, 1..2 if it was forcefully
        #          removed, and 0, if it could not be removed.
        lock = "%s.lock" % (fn)

        # Let's open file and check its pid
        try:
            input_file = open(lock, 'r')
        except:
            pass
        else:
            file_pid = input_file.readline()
            file_pid = file_pid.strip()
            input_file.close()

            # Let's check if said pid is still around
            try:
                os.kill(int(file_pid), 0)
            except:
                # Process is not around anymore
                if file_pid.isdigit():
                    uniq = "%s.%d" % (fn, int(file_pid))
                    logger.info("lock-owner %d found dead, removing lock!" % (int(file_pid)))
                    os.unlink(lock)
                    try:
                        # Also try to remove uniq file
                        os.unlink(uniq)
                    except:
                        pass
                    # Lock should be broken now
                    return True
                else:
                    logger.warn("error: cannot determine process id from lock file!")
            else:
                logger.info("lock-owned %d still lives..." % (int(file_pid)))

        # Was not able to break lock
        return False

    def create_lock(self, fn):
        """
        This function attempts to create a file lock around the
        specified filename according to Linux conventions. It first
        creates a unique file using the process id as unique suffix,
        then attempts to hardlink said file to the filename plus
        suffix <.lock>. The attempt is randomly backed off to retry
        on failure to hardlink. Additionally, the link count is
        checked to detect hidden success.

        This is a blocking function, and may block indefinitely on
        dead-locks, despite occasional lock acquiry wake-ups.
        """
        # purpose: blockingly wait for lock file creation
        # paramtr: $fn (IN): name of file to create lock file for
        # returns: 1: lock was created.
        retries = 0
        to_wait = 0

        while not self.create_lockfile(fn):
            if retries > 10:
                # We waited enough, let's try to break the lock
                self.break_lock(fn)
                retries = 0 # Shouldn't be necessary, just in case
            else:
                # Let's wait for a little while
                to_wait = 5 * random.random()
                logger.info("lock on file %s is busy, retry %d, waiting %.1f s..." % (fn, retries, to_wait))
                time.sleep(to_wait)
                retries = retries + 1

        logger.info("obtained lock for %s" % (fn))
        return True

    def delete_lock(self, fn):
        """
        This static function deletes all lock files around the given
        filename.  It should be a fast function, as no waiting is
        required.
        """
        # purpose: removes a lock file NFS-reliably
        # paramtr: $fn (IN): name of main file to lock
        # returns: 1 or 2 on success, 0 on failure to unlock
        tolock = fn
        lock = "%s.lock" % (tolock)
        uniq = "%s.%d" % (tolock, self.m_pid)
        result = 0

        try:
            os.unlink(lock)
        except:
            pass
        else:
            result = result + 1
            at_exit.pop(lock)            

        try:
            os.unlink(uniq)
        except:
            pass
        else:
            result = result + 1
            at_exit.pop(uniq)            

        return result

    def filename(self):
        """
        This is a simple accessor function, returning the filename
        that was passed to the constructor.
        """
        # purpose: returns the name of the communication file
        return self.m_filename

    def dbtie(self, ro=False):
        """
        This member increases the lock count for the database file,
        and connects to the database file.

        The return value is the result of the open call. It may be
        None in case of failure to open the database.
        """
        # purpose: Lock a file and tie it to a hash
        # paramtr: $ro (opt. IN): if true, open in read-only mode
        # returns: None on error, underlying object otherwise

        # Create key if not already there
        if self.m_pid not in self.m_count:
            self.m_count[self.m_pid] = 0

        if self.m_count[self.m_pid] == 0:
            self.create_lock(self.m_filename)
            self.m_count[self.m_pid] = self.m_count[self.m_pid] + 1

        # Open database in read only or read/write mode
        if ro:
            my_mode = 'r'
        else:
            my_mode = 'c'

        try:
            my_db = anydbm.open(self.m_filename, my_mode)
        except:
            # Remove lock on failure to connect
            self.m_count[self.m_pid] = self.m_count[self.m_pid] - 1
            if self.m_count[self.m_pid] == 0:
                self.delete_lock(self.m_filename)
            return None

        return my_db

    def locked(self):
        """
        This function returns the reference count for locks on the
        file. Refernce counters are kept on a per-process basis. This
        is not thread safe.
        """
        # purpose: detects already tied databases
        # returns: reference count for lock
        if self.m_pid not in self.m_count:
            return 0
        
        return self.m_count[self.m_pid]

    def dbuntie(self, dbref):
        """
        This function closes the hash data base and relinquishes the
        lock. This method should only be called, if the previous dbtie
        operation was successful, similar to opening and closing file
        handles
        """
        # purpose: untie a hash and release the lock
        # paramtr: $dbref (I): reference to db to be closed
        # returns: -
        self.m_count[self.m_pid] = self.m_count[self.m_pid] - 1
        if self.m_count[self.m_pid] == 0:
            self.delete_lock(self.m_filename)

        # Close datbase
        try:
            dbref.close()
        except:
            logger.warn("Error closing %s database" % (m_filename))

    def clone(self):
        """
        This is a comprehensive function to copy all values from the
        database into memory. Please note that you can create nasty
        dead-locks this way
        """
        # purpose: obtains all current values into a copy
        # returns: a hash with key => value, may be empty
        #          if no keys in database, or None if error
        my_copy = {}
        my_db = self.dbtie()

        if my_db is not None:
            # Copy each key/value pair, converting the value to int
            for key in my_db.keys():
                my_copy[key] = int(my_db[key])

            # All done
            self.dbuntie(my_db)

            return my_copy

        return None

    def inc(self, key, incr=1):
        # purpose: increment the count for a site handle
        # paramtr: $key (IN): key of value to increment
        #          $incr (opt. IN): increment, defaults to 1
        # returns: new value, None on error
        if key is None:
            return None

        # Just in case key is not string
        key = str(key)

        my_db = self.dbtie()

        if my_db is not None:
            if key in my_db:
                val = int(my_db[key])
                val = val + incr
            else:
                val = incr

            # Write new value
            my_db[key] = str(val)

            # Done, disconnect from data base
            self.dbuntie(my_db)

            return val

        return None

    def dec(self, key, decr=1):
        # purpose: decrement the count for a site handle
        # paramtr: $key (IN): key of value to decrement
        #          $decr (opt. IN): decrement, defaults to 1
        # returns: new value, None in case of error
        if key is None:
            return None

        # Just in case key is not string
        key = str(key)

        my_db = self.dbtie()

        if my_db is not None:
            if key in my_db:
                val = int(my_db[key])
                val = val - decr
            else:
                val = decr

            # Write new value
            my_db[key] = str(val)

            # Done, disconnect from data base
            self.dbuntie(my_db)

            return val

        return None

# Register module exit handler
atexit.register(intent_exit_handler)

# Built-in testing
if __name__ == '__main__':
    a = Intent()
    a.new("/tmp/test1")

    # Test tie/untie
    b = a.dbtie()
    a.dbuntie(b)

    # Increment keys
    a.inc('usc')
    a.inc('usc')
    c = a.inc('usc')
    if c is None:
        print("Cannot get counter!")
    else:
        print("Counter is now %d" % (c))
    c = a.dec("usc", 3)
    if c is None:
        print("Cannot get counter!")
    else:
        print("Counter is now %d" % (c))

    my_dict = {}
    my_dict = a.clone()
    for key in my_dict.keys():
        print(key, "-->", my_dict[key])
    print("done")
