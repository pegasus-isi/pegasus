/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.vdl.router;

import java.util.*;
import org.griphyn.vdl.util.Logging;

/**
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Cache {
    /** remember how long to save a cache entry. */
    long m_ttl = 0;

    /** Interior class to encapsulate cached objects and their additional management keys. */
    public class CacheEntry {
        /** This is the cached object. */
        Object m_value;

        /** This is expiration date of the object. */
        long m_expire;

        /**
         * Constructs a cache item with its management data. The time to live is determined from the
         * member variable.
         *
         * @param value is the object to be cached.
         */
        CacheEntry(Object value) {
            this.m_value = value;
            this.m_expire = System.currentTimeMillis() + m_ttl;
        }
    }

    /**
     * remember the objects to cache for. The cache consists of a concise key to locate any object,
     * a value for the located large object, and a lifetime for the object.
     */
    java.util.Map m_cache = null;

    /** Maintains statistics. */
    static long[] m_stats = null;

    /**
     * ctor: Initialize the base functionalities of the cache.
     *
     * @param ttl is the lifetime of a positive entry in seconds.
     */
    public Cache(int ttl) {
        this.m_ttl = 1000 * ttl;
        this.m_cache = new java.util.HashMap();
        if (m_stats == null) {
            // Singleton:
            Cache.m_stats = new long[5]; // insert, update, miss, expired, hit

            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread() {
                                public void run() {
                                    Logging.instance()
                                            .log(
                                                    "cache",
                                                    0,
                                                    "ins="
                                                            + Cache.m_stats[0]
                                                            + ",updt="
                                                            + Cache.m_stats[1]
                                                            + ",miss="
                                                            + Cache.m_stats[2]
                                                            + ",hit="
                                                            + Cache.m_stats[4]);
                                }
                            });
        }
    }

    /**
     * Enters a value into the cache.
     *
     * @param key is a concise, unique description of the object.
     * @param value is the object to be cached.
     * @return <code>null</code> for a fresh object, or the old CacheEntry.
     */
    public Object set(Object key, Object value) {
        CacheEntry ce = (CacheEntry) this.m_cache.put(key, new CacheEntry(value));
        this.m_stats[ce == null ? 0 : 1]++; // count insert or update
        return (ce == null ? null : ce.m_value);
    }

    /**
     * Requests an item from the cache.
     *
     * @param key is the descriptor of the object.
     */
    public Object get(Object key) {
        CacheEntry ce = (CacheEntry) this.m_cache.get(key);

        // new object?
        if (ce == null) {
            this.m_stats[2]++; // count MISS
            return null;
        }

        // expired object?
        if (ce.m_expire < System.currentTimeMillis()) {
            this.m_stats[3]++; // count EXPIRED
            this.m_cache.remove(key);
            return null;
        }

        // known object!
        this.m_stats[4]++;
        return ce.m_value;
    }

    /**
     * Requests a copy of the statistics counters.
     *
     * @return the counter values.
     */
    public long[] getStatistics() {
        long[] result = new long[5];
        System.arraycopy(this.m_stats, 0, result, 0, 5);
        return result;
    }
}
