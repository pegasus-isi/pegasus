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
import edu.isi.pegasus.common.util.XMLWriter;

/**
 *
 * @author gmehta
 */
public class AbstractJob {

    protected List mArguments;
    protected List<Profile> mProfiles;
    protected File mStdin;
    protected File mStdout;
    protected File mStderr;
    protected List<File> mUses;
    protected List<Invoke> mInvokes;
    protected String mName;
    protected String mId;
    protected String mNamespace;
    protected String mVersion;
    protected String mNodeLabel;
    protected static LogManager mLogger;

    protected AbstractJob() {
        mLogger = LogManagerFactory.loadSingletonInstance();
        mArguments = new LinkedList();
        mUses = new LinkedList<File>();
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

    public List getArguments() {
        return Collections.unmodifiableList(mArguments);
    }

    public AbstractJob addArgument(String argument) {
        if (argument != null) {
            mArguments.add(argument);
        }
        return this;
    }

    public AbstractJob addArgument(File file) {
        if (file != null) {
            mArguments.add(file);
        }
        return this;
    }

//    public Profiles addProfile(String namespace, String key, String value) {
//        mProfiles.addProfileDirectly(namespace, key, value);
//        return mProfiles;
//
//    }
//
//        public Profiles addProfile(Profiles.NAMESPACES namespace, String key, String value) {
//        mProfiles.addProfileDirectly(namespace, key, value);
//        return mProfiles;
//    }
    public AbstractJob addProfile(String namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;

    }

    public AbstractJob addProfile(Profile.NAMESPACE namespace, String key,
            String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    public File getStdin() {
        return mStdin;
    }

    public AbstractJob setStdin(File stdin) {
        mStdin = stdin;
        mStdin.setLink(File.LINK.INPUT);
        return this;
    }

    public File getStdout() {
        return mStdout;
    }

    public AbstractJob setStdout(File stdout) {
        mStdout = stdout;
        mStdout.setLink(File.LINK.OUTPUT);
        return this;
    }

    public File getStderr() {
        return mStderr;
    }

    public AbstractJob setStderr(File stderr) {
        mStderr = stderr;
        mStderr.setLink(File.LINK.OUTPUT);
        return this;
    }

    public List<File> getUses() {
        return Collections.unmodifiableList(mUses);
    }

    public AbstractJob addUses(File file) {
        mUses.add(file);
        return this;
    }

    public AbstractJob addUses(List<File> files) {
        mUses.addAll(files);
        return this;
    }

    public List<Invoke> getInvoke() {
        return Collections.unmodifiableList(mInvokes);
    }

    public AbstractJob addInvoke(Invoke.WHEN when, String what) {
        Invoke i = new Invoke(when, what);
        mInvokes.add(i);
        return this;
    }

    public AbstractJob addInvoke(Invoke invoke) {
        mInvokes.add(invoke);
        return this;
    }

    public AbstractJob addInvoke(List<Invoke> invokes) {
        this.mInvokes.addAll(invokes);
        return this;
    }

    public String getName() {
        return mName;
    }

    public String getId() {
        return mId;
    }

    public String getNodeLabel() {
        return mNodeLabel;
    }

    public void setNodeLabel(String label) {
        this.mNodeLabel = label;
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {

        Class c = this.getClass();
        //Check if its a dax, dag or job class
        if (c == DAX.class) {
            writer.startElement("dax", indent);
        } else if (c == DAG.class) {
            writer.startElement("dag", indent);
        } else if (c == Job.class) {
            writer.startElement("job", indent);

        }
        //add job attributes
        writer.writeAttribute("id", mId);
        if (c == Job.class && mNamespace != null && !mNamespace.isEmpty()) {
            writer.writeAttribute("namespace", mNamespace);
        }
        writer.writeAttribute("name", mName);
        if (c == Job.class && mVersion != null && !mVersion.isEmpty()) {
            writer.writeAttribute("version", mVersion);
        }
        if (mNodeLabel != null && !mNodeLabel.isEmpty()) {
            writer.writeAttribute("node-label", mNodeLabel);
        }
        //add argument
        if (!mArguments.isEmpty()) {
            writer.startElement("argument", indent + 1);
            for (Object o : mArguments) {
                if (o.getClass() == String.class) {
                    //if class is string add argument string in the data section
                    writer.writeData((String) o);
                }
                if (o.getClass() == File.class) {
                    //add file tags in the argument elements data section
                    ((File) o).toXML(writer, 0, "argument");
                }
            }
            writer.endElement();
        }
        //add profiles
        for (Profile p : mProfiles) {
            p.toXML(writer, indent + 1);
        }
        //add stdin
        if (mStdin != null) {
            mStdin.toXML(writer, indent + 1, "stdin");
        }
        //add stdout
        if (mStdout != null) {
            mStdout.toXML(writer, indent + 1, "stdout");
        }
        //add stderr
        if (mStderr != null) {
            mStderr.toXML(writer, indent + 1, "stderr");
        }
        //add uses
        for (File f : mUses) {
            f.toXML(writer, indent + 1, "uses");
        }
        //add invoke
        for (Invoke i : mInvokes) {
            i.toXML(writer, indent + 1);
        }
        if (!(mUses.isEmpty() && mInvokes.isEmpty() && mStderr == null && mStdout == null && mStdin == null && mProfiles.isEmpty() && mArguments.isEmpty())) {
            writer.endElement(indent);
        } else {
            writer.endElement();
        }

    }
}
