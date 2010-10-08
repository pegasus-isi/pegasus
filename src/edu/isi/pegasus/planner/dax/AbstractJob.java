package edu.isi.pegasus.planner.dax;

import java.util.*;
//import edu.isi.pegasus.planner.catalog.classes.*;

import edu.isi.pegasus.common.logging.*;
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
       Class c = this.getClass();
       //Check if its a dax, dag or job class
        if (c==DAX.class){
            writer.startElement("dax");
        }else if (c==DAG.class){
            writer.startElement("dag");
        } else if (c==Job.class){
            writer.startElement("job");
            
        }
        //add job attributes
        writer.writeAttribute("id", mId);
        if (c==Job.class && mNamespace != null && !mNamespace.isEmpty()) {
            writer.writeAttribute("namespace", mNamespace);
        }
        writer.writeAttribute("name", mName);
        if (c==Job.class && mVersion != null && !mVersion.isEmpty()) {
            writer.writeAttribute("version", mVersion);
        }
        if (mNodeLabel != null && !mNodeLabel.isEmpty()) {
            writer.writeAttribute("node-label", mNodeLabel);
        }
        //add argument
        if (!mArguments.isEmpty()) {
            writer.startElement("argument");
            for (Object o : mArguments) {
                if (o.getClass() == String.class) {
                    //if class is string add argument string in the data section
                    writer.writeData(" "+(String) o);
                }
                if (o.getClass() == File.class) {
                    //add file tags in the argument elements data section
                    ((File) o).toXML(writer, "argument");
                }
            }
            writer.endElement();
        }
        //add profiles
        for (Profile p : mProfiles) {
            p.toXML(writer);
        }
        //add stdin
        if(mStdin!=null){
            mStdin.toXML(writer, "stdin");
        }
        //add stdout
        if(mStdout!=null){
            mStdout.toXML(writer, "stdout");
        }
        //add stderr
        if(mStderr!=null){
            mStderr.toXML(writer, "stderr");
        }
        //add uses
        for (File f : mUses) {
            f.toXML(writer, "uses");
        }
        //add invoke
        for (Invoke i : mInvokes) {
            i.toXML(writer);
        }
        writer.endElement();

    }
}
