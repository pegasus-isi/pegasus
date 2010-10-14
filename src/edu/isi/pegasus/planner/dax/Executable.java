/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.util.XMLWriter;

/**
 *
 * @author gmehta
 */
public class Executable extends CatalogType {

    public static enum ARCH {

        x86, x86_64, ppc, ppc_64, ia64, sparcv7, sparcv9
    }

    public static enum OS {

        LINUX, SUNOS, AIX, MACOS, DARWIN, WINDOWS, SOLARIS
    }
    protected String mNamespace;
    protected String mName;
    protected String mVersion;
    protected ARCH mArch;
    protected OS mOs;
    protected String mOsRelease;
    protected String mOsVersion;
    protected String mGlibc;

    public Executable(String name) {
        this("", name, "");
    }

    public Executable(String namespace, String name, String version) {
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;
        mVersion = (version == null) ? "" : null;

    }

    public String getName() {
        return mName;
    }

    public String getNamespace() {
        return mNamespace;
    }

    public String getVersion() {
        return mVersion;
    }

    public Executable setArchitecture(ARCH arch) {
        mArch = arch;
        return this;
    }

    public Executable setOS(OS os) {
        mOs = os;
        return this;
    }

    public Executable setOSRelease(String osrelease) {
        mOsRelease = osrelease;
        return this;
    }

    public Executable setOSVersion(String osversion) {
        mOsVersion = osversion;
        return this;
    }

    public Executable setGlibc(String glibc) {
        mGlibc = glibc;
        return this;
    }

    public ARCH getARCH() {
        return mArch;
    }

    public OS getOS() {
        return mOs;
    }

    public String getOsRelease() {
        return (mOsRelease == null) ? "" : mOsRelease;
    }

    public String getOsVersion() {
        return (mOsVersion == null) ? "" : mOsVersion;
    }

    public String getGlibc() {
        return (mGlibc == null) ? "" : mGlibc;
    }

    @Override
    public void toXML(XMLWriter writer) {
        writer.startElement("executable");
        writer.incIndent();
        if (mNamespace != null && !mNamespace.isEmpty()) {
            writer.writeAttribute("namespace", mNamespace);
        }
        writer.writeAttribute("name", mName);
        if (mVersion != null && !mVersion.isEmpty()) {
            writer.writeAttribute("version", mVersion);
        }
        if(mArch!=ARCH.x86){
            writer.writeAttribute("arch", mArch.toString().toLowerCase());
        }
                if(mOs!=OS.LINUX){
            writer.writeAttribute("os", mOs.toString().toLowerCase());
        }
        if(mOsRelease!=null && !mOsRelease.isEmpty()){
            writer.writeAttribute("osrelease", mOsRelease);
        }
        if(mOsVersion!=null && !mOsVersion.isEmpty()){
            writer.writeAttribute("osversion", mOsVersion);
        }
        if(mGlibc!=null && !mGlibc.isEmpty()){
            writer.writeAttribute("glibc", mGlibc);
        }
        super.toXML(writer);
        writer.decIndent();
        writer.endElement();

    }
}
