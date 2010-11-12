/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.dax;

import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.XMLWriter;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 *
 * @author gmehta
 * @version $Revision$
 */
public class AbstractJob {

    protected List mArguments;
    protected List<Profile> mProfiles;
    protected File mStdin;
    protected File mStdout;
    protected File mStderr;
    protected Set<File> mUses;
    protected List<Invoke> mInvokes;
    protected String mName;
    protected String mId;
    protected String mNamespace;
    protected String mVersion;
    protected String mNodeLabel;
    protected static LogManager mLogger;
    private static final String ARG_DELIMITER = " ";
    private static final String FILE_DELIMITER = " ";

    protected AbstractJob() {
        mLogger = LogManagerFactory.loadSingletonInstance();
        mArguments = new LinkedList();
        mUses = new LinkedHashSet<File>();
        mInvokes = new LinkedList<Invoke>();
        mProfiles = new LinkedList<Profile>();
    }

    protected static void checkID(String id) {
        if (!Patterns.isNodeIdValid(id)) {
            mLogger.log(
                    "Id: " + id + " should of the type [A-Za-z0-9][-A-Za-z0-9]*",
                    LogManager.ERROR_MESSAGE_LEVEL);
        }
    }

    /**
     *
     * @return
     */
    public List getArguments() {
        return Collections.unmodifiableList(mArguments);
    }

    /**
     *
     * @param argument
     * @return
     */
    public AbstractJob addArgument(String argument) {
        if (argument != null) {
            if (!mArguments.isEmpty()) {
                mArguments.add(ARG_DELIMITER);
            }
            mArguments.add(argument);
        }
        return this;
    }

    /**
     *
     * @param file
     * @return
     */
    public AbstractJob addArgument(File file) {
        if (file != null) {
            if (!mArguments.isEmpty()) {
                mArguments.add(ARG_DELIMITER);
            }
            mArguments.add(file);
        }
        return this;
    }

    /**
     *
     * @param files
     * @return
     */
    public AbstractJob addArgument(File[] files) {
        this.addArgument(files, FILE_DELIMITER);
        return this;
    }

    /**
     *
     * @param files
     * @return
     */
    public AbstractJob addArgument(List<File> files) {
        this.addArgument(files, FILE_DELIMITER);
        return this;
    }

    /**
     *
     * @param files
     * @param filedelimiter
     * @return
     */
    public AbstractJob addArgument(File[] files, String filedelimiter) {
        filedelimiter = (filedelimiter == null) ? FILE_DELIMITER : filedelimiter;
        if (files != null && files.length > 0) {
            if (!mArguments.isEmpty()) {
                mArguments.add(ARG_DELIMITER);
            }
            boolean first = true;
            for (File f : files) {
                if (!first) {
                    mArguments.add(filedelimiter);
                }
                mArguments.add(f);
                first = false;
            }
        }
        return this;
    }

    /**
     *
     * @param files
     * @param filedelimiter
     * @return
     */
    public AbstractJob addArgument(List<File> files, String filedelimiter) {
        if (files != null && !files.isEmpty()) {
            this.addArgument((File[]) files.toArray(), filedelimiter);
        }
        return this;
    }

    /**
     *
     * @param argkey
     * @param argvalue
     * @return
     */
    public AbstractJob addArgument(String argkey, String argvalue) {
        this.addArgument(argkey, argvalue, ARG_DELIMITER);
        return this;
    }

    /**
     *
     * @param argkey
     * @param argvalue
     * @param argdelimiter
     * @return
     */
    public AbstractJob addArgument(String argkey, String argvalue,
            String argdelimiter) {
        argdelimiter = (argdelimiter == null) ? ARG_DELIMITER : argdelimiter;
        if (argkey != null && argvalue != null) {
            this.addArgument(argkey + argdelimiter + argvalue);
        }
        return this;

    }

    /**
     *
     * @param argkey
     * @param argvalue
     * @return
     */
    public AbstractJob addArgument(String argkey, File argvalue) {
        this.addArgument(argkey, argvalue, ARG_DELIMITER);
        return this;
    }

    /**
     *
     * @param argkey
     * @param argvalue
     * @param argdelimiter
     * @return
     */
    public AbstractJob addArgument(String argkey, File argvalue,
            String argdelimiter) {
        argdelimiter = (argdelimiter == null) ? ARG_DELIMITER : argdelimiter;
        if (argkey != null && argvalue != null) {
            this.addArgument(argkey + argdelimiter);
            mArguments.add(argvalue);
        }
        return this;
    }

    /**
     *
     * @param argkey
     * @param argvalue
     * @return
     */
    public AbstractJob addArgument(String argkey, File[] argvalue) {
        this.addArgument(argkey, argvalue, ARG_DELIMITER, FILE_DELIMITER);
        return this;
    }

    /**
     *
     * @param argkey
     * @param argvalue
     * @return
     */
    public AbstractJob addArgument(String argkey, List<File> argvalue) {
        this.addArgument(argkey, argvalue, ARG_DELIMITER, FILE_DELIMITER);
        return this;
    }

    /**
     *
     * @param argkey
     * @param argvalue
     * @param argdelimiter
     * @param filedelimiter
     * @return
     */
    public AbstractJob addArgument(String argkey, File[] argvalue,
            String argdelimiter, String filedelimiter) {
        argdelimiter = (argdelimiter == null) ? ARG_DELIMITER : argdelimiter;
        filedelimiter = (filedelimiter == null) ? FILE_DELIMITER : filedelimiter;

        if (argkey != null && argvalue != null && argvalue.length > 0) {
            this.addArgument(argkey + argdelimiter);
            boolean first = true;
            for (File f : argvalue) {
                if (!first) {
                    mArguments.add(filedelimiter);
                }
                mArguments.add(f);
                first = false;
            }
        }
        return this;
    }

    /**
     *
     * @param argkey
     * @param argvalue
     * @param argdelimiter
     * @param filedelimiter
     * @return
     */
    public AbstractJob addArgument(String argkey, List<File> argvalue,
            String argdelimiter, String filedelimiter) {
        if (argkey != null && argvalue != null && !argvalue.isEmpty()) {
            this.addArgument(argkey, (File[]) argvalue.toArray(), argdelimiter,
                    filedelimiter);
        }
        return this;
    }

    /**
     *
     * @param namespace
     * @param key
     * @param value
     * @return
     */
    public AbstractJob addProfile(String namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));


        return this;



    }

    /**
     *
     * @param namespace
     * @param key
     * @param value
     * @return
     */
    public AbstractJob addProfile(Profile.NAMESPACE namespace, String key,
            String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    /**
     *
     * @return
     */
    public File getStdin() {
        return mStdin;


    }

    /**
     *
     * @param stdin
     * @return
     */
    public AbstractJob setStdin(File stdin) {
        File f = new File(stdin, File.LINK.INPUT);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @param transfer
     * @return
     */
    public AbstractJob setStdin(File stdin, File.TRANSFER transfer) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @param register
     * @return
     */
    public AbstractJob setStdin(File stdin, boolean register) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setRegister(register);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @param transfer
     * @param register
     * @return
     */
    public AbstractJob setStdin(File stdin, File.TRANSFER transfer,
            boolean register) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @param transfer
     * @param register
     * @param optional
     * @return
     */
    public AbstractJob setStdin(File stdin, File.TRANSFER transfer,
            boolean register, boolean optional) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @return
     */
    public AbstractJob setStdin(String stdin) {
        File f = new File(stdin, File.LINK.INPUT);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @param transfer
     * @return
     */
    public AbstractJob setStdin(String stdin, File.TRANSFER transfer) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @param register
     * @return
     */
    public AbstractJob setStdin(String stdin, boolean register) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setRegister(register);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @param transfer
     * @param register
     * @return
     */
    public AbstractJob setStdin(String stdin, File.TRANSFER transfer,
            boolean register) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdin
     * @param transfer
     * @param register
     * @param optional
     * @return
     */
    public AbstractJob setStdin(String stdin, File.TRANSFER transfer,
            boolean register, boolean optional) {
        File f = new File(stdin, File.LINK.INPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStdin = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @return
     */
    public File getStdout() {
        return mStdout;
    }

    /**
     *
     * @param stdout
     * @return
     */
    public AbstractJob setStdout(File stdout) {
        File f = new File(stdout, File.LINK.OUTPUT);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdout
     * @param transfer
     * @return
     */
    public AbstractJob setStdout(File stdout, File.TRANSFER transfer) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdout
     * @param register
     * @return
     */
    public AbstractJob setStdout(File stdout, boolean register) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setRegister(register);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdout
     * @param transfer
     * @param register
     * @return
     */
    public AbstractJob setStdout(File stdout, File.TRANSFER transfer,
            boolean register) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdout
     * @param transfer
     * @param register
     * @param optional
     * @return
     */
    public AbstractJob setStdout(File stdout, File.TRANSFER transfer,
            boolean register, boolean optional) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdout
     * @return
     */
    public AbstractJob setStdout(String stdout) {
        File f = new File(stdout, File.LINK.OUTPUT);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdout
     * @param transfer
     * @return
     */
    public AbstractJob setStdout(String stdout, File.TRANSFER transfer) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdout
     * @param register
     * @return
     */
    public AbstractJob setStdout(String stdout, boolean register) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setRegister(register);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stdout
     * @param transfer
     * @param register
     * @return
     */
    public AbstractJob setStdout(String stdout, File.TRANSFER transfer,
            boolean register) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStdout = f;
        mUses.add(f);
        return this;
    }

    /**
     *
     * @param stdout
     * @param transfer
     * @param register
     * @param optional
     * @return
     */
    public AbstractJob setStdout(String stdout, File.TRANSFER transfer,
            boolean register, boolean optional) {
        File f = new File(stdout, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStdout = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @return
     */
    public File getStderr() {
        return mStderr;

    }

    /**
     * 
     * @param stderr
     * @return
     */
    public AbstractJob setStderr(File stderr) {
        File f = new File(stderr, File.LINK.OUTPUT);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stderr
     * @param transfer
     * @return
     */
    public AbstractJob setStderr(File stderr, File.TRANSFER transfer) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stderr
     * @param register
     * @return
     */
    public AbstractJob setStderr(File stderr, boolean register) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setRegister(register);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stderr
     * @param transfer
     * @param register
     * @return
     */
    public AbstractJob setStderr(File stderr, File.TRANSFER transfer,
            boolean register) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stderr
     * @param transfer
     * @param register
     * @param optional
     * @return
     */
    public AbstractJob setStderr(File stderr, File.TRANSFER transfer,
            boolean register, boolean optional) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stderr
     * @return
     */
    public AbstractJob setStderr(String stderr) {
        File f = new File(stderr, File.LINK.OUTPUT);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     * 
     * @param stderr
     * @param transfer
     * @return
     */
    public AbstractJob setStderr(String stderr, File.TRANSFER transfer) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stderr
     * @param register
     * @return
     */
    public AbstractJob setStderr(String stderr, boolean register) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setRegister(register);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stderr
     * @param transfer
     * @param register
     * @return
     */
    public AbstractJob setStderr(String stderr, File.TRANSFER transfer,
            boolean register) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @param stderr
     * @param transfer
     * @param register
     * @param optional
     * @return
     */
    public AbstractJob setStderr(String stderr, File.TRANSFER transfer,
            boolean register, boolean optional) {
        File f = new File(stderr, File.LINK.OUTPUT);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        mStderr = f;
        if (!mUses.contains(f)) {
            mUses.add(f);
        }
        return this;
    }

    /**
     *
     * @return
     */
    public Set<File> getUses() {
        return Collections.unmodifiableSet(mUses);
    }

    /**
     *
     *
     *
     *
     * @param file
     * @param link
     * @return
     */
    public AbstractJob uses(String file, File.LINK link) {
        File f = new File(file, link);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion) + "already contains a file " + Separator.
                    combine(f.mNamespace, f.mName, f.mVersion) + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     *
     * @param file
     * @param link
     * @param register
     * @return
     */
    public AbstractJob uses(String file, File.LINK link, boolean register) {
        File f = new File(file, link);
        f.setRegister(register);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion) + "already contains a file " + Separator.
                    combine(f.mNamespace, f.mName, f.mVersion) + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     *
     * @param file
     * @param link
     * @param transfer
     * @return
     */
    public AbstractJob uses(String file, File.LINK link, File.TRANSFER transfer) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion) + "already contains a file " + Separator.
                    combine(f.mNamespace, f.mName, f.mVersion) + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     *
     * @param file
     * @param link
     * @param transfer
     * @param register
     * @return
     */
    public AbstractJob uses(String file, File.LINK link, File.TRANSFER transfer,
            boolean register) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setTransfer(transfer);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion) + "already contains a file " + Separator.
                    combine(f.mNamespace, f.mName, f.mVersion) + ". Ignoring",
                    LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * 
     * @param file
     * @param link
     * @param transfer
     * @param register
     * @param optional
     * @param executable
     * @return
     */
    public AbstractJob uses(String file, File.LINK link, File.TRANSFER transfer,
            boolean register, boolean optional, boolean executable) {
        File f = new File(file, link);
        f.setRegister(register);
        f.setOptional(optional);
        f.setTransfer(transfer);
        f.setExecutable(executable);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion)
                    + "already contains a file " + Separator.combine(
                    f.mNamespace, f.mName, f.mVersion)
                    + ". Ignoring", LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     *
     * @param file
     * @param link
     * @return
     */
    public AbstractJob uses(File file, File.LINK link) {
        File f = new File(file, link);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion)
                    + "already contains a file " + Separator.combine(
                    f.mNamespace, f.mName, f.mVersion)
                    + ". Ignoring", LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * 
     * @param file
     * @param link
     * @param transfer
     * @return
     */
    public AbstractJob uses(File file, File.LINK link, File.TRANSFER transfer) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion)
                    + "already contains a file " + Separator.combine(
                    f.mNamespace, f.mName, f.mVersion)
                    + ". Ignoring", LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     * 
     * @param file
     * @param link
     * @param register
     * @return
     */
    public AbstractJob uses(File file, File.LINK link, boolean register) {
        File f = new File(file, link);
        f.setRegister(register);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion)
                    + "already contains a file "
                    + Separator.combine(f.mNamespace, f.mName, f.mVersion)
                    + ". Ignoring", LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;


    }

    /**
     *
     * @param file
     * @param link
     * @param transfer
     * @param register
     * @return
     */
    public AbstractJob uses(File file, File.LINK link, File.TRANSFER transfer,
            boolean register) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setRegister(register);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion)
                    + "already contains a file " + Separator.combine(
                    f.mNamespace, f.mName, f.mVersion)
                    + ". Ignoring", LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     *
     * @param file
     * @param link
     * @param transfer
     * @param register
     * @param optional
     * @param executable
     * @return
     */
    public AbstractJob uses(File file, File.LINK link, File.TRANSFER transfer,
            boolean register, boolean optional, boolean executable) {
        File f = new File(file, link);
        f.setTransfer(transfer);
        f.setRegister(register);
        f.setOptional(optional);
        f.setExecutable(executable);
        if (!mUses.contains(f)) {
            mUses.add(f);
        } else {
            mLogger.log("Job " + Separator.combine(mNamespace, mName, mVersion)
                    + "already contains a file " + Separator.combine(
                    f.mNamespace, f.mName, f.mVersion)
                    + ". Ignoring", LogManager.WARNING_MESSAGE_LEVEL);
        }
        return this;
    }

    /**
     *
     * @param files
     * @param link
     * @return
     */
    public AbstractJob uses(List<File> files, File.LINK link) {
        for (File file : files) {
            File f = new File(file, link);
            if (!mUses.contains(f)) {
                mUses.add(f);
            } else {
                mLogger.log("Job " + Separator.combine(mNamespace, mName,
                        mVersion)
                        + "already contains a file " + Separator.combine(
                        f.mNamespace, f.mName, f.mVersion)
                        + ". Ignoring", LogManager.WARNING_MESSAGE_LEVEL);
            }
        }
        return this;
    }

    /**
     *
     * @return
     */
    public List<Invoke> getInvoke() {
        return Collections.unmodifiableList(mInvokes);
    }

    /**
     * 
     * @param when
     * @param what
     * @return
     */
    public AbstractJob addInvoke(Invoke.WHEN when, String what) {
        Invoke i = new Invoke(when, what);
        mInvokes.add(i);
        return this;
    }

    /**
     *
     * @param invoke
     * @return
     */
    public AbstractJob addInvoke(Invoke invoke) {
        mInvokes.add(invoke);
        return this;
    }

    /**
     *
     * @param invokes
     * @return
     */
    public AbstractJob addInvoke(List<Invoke> invokes) {
        this.mInvokes.addAll(invokes);
        return this;
    }

    /**
     * 
     * @return
     */
    public String getName() {
        return mName;
    }

    /**
     *
     * @return
     */
    public String getId() {
        return mId;
    }

    /**
     *
     * @return
     */
    public String getNodeLabel() {
        return mNodeLabel;
    }

    /**
     *
     * @param label
     */
    public void setNodeLabel(String label) {
        this.mNodeLabel = label;
    }

    /**
     *
     * @param writer
     */
    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    /**
     * 
     * @param writer
     * @param indent
     */
    public void toXML(XMLWriter writer, int indent) {
        Class c = this.getClass();
        //Check if its a dax, dag or job class
        if (c == DAX.class) {
            writer.startElement(
                    "dax", indent);
            writer.writeAttribute(
                    "id", mId);
            writer.writeAttribute(
                    "file", mName);
        } else if (c == DAG.class) {
            writer.startElement(
                    "dag", indent);
            writer.writeAttribute(
                    "id", mId);
            writer.writeAttribute(
                    "file", mName);
        } else if (c == Job.class) {
            writer.startElement(
                    "job", indent);
            writer.writeAttribute(
                    "id", mId);
            if (mNamespace != null && !mNamespace.isEmpty()) {
                writer.writeAttribute("namespace", mNamespace);
            }
            writer.writeAttribute("name", mName);
            if (mVersion != null && !mVersion.isEmpty()) {
                writer.writeAttribute("version", mVersion);
            }
        }
        if (mNodeLabel != null && !mNodeLabel.isEmpty()) {
            writer.writeAttribute("node-label", mNodeLabel);
        } //add argument
        if (!mArguments.isEmpty()) {
            writer.startElement("argument", indent + 1);
            for (Object o : mArguments) {
                if (o.getClass() == String.class) {
                    //if class is string add argument string in the data section
                    writer.writeData(
                            (String) o);
                }
                if (o.getClass() == File.class) {
                    //add file tags in the argument elements data section
                    ((File) o).toXML(writer, 0, "argument");
                }
            }
            writer.endElement();
        } //add profiles
        for (Profile p : mProfiles) {
            p.toXML(writer, indent + 1);
        } //add stdin
        if (mStdin != null) {
            mStdin.toXML(writer, indent + 1, "stdin");
        } //add stdout
        if (mStdout != null) {
            mStdout.toXML(writer, indent + 1, "stdout");
        } //add stderr
        if (mStderr != null) {
            mStderr.toXML(writer, indent + 1, "stderr");
        } //add uses
        for (File f : mUses) {
            f.toXML(writer, indent + 1, "uses");
        } //add invoke
        for (Invoke i : mInvokes) {
            i.toXML(writer, indent + 1);
        }
        if (!(mUses.isEmpty() && mInvokes.isEmpty() && mStderr == null && mStdout == null && mStdin == null && mProfiles.
                isEmpty() && mArguments.isEmpty())) {
            writer.endElement(indent);
        } else {
            writer.endElement();

        }

    }
}
