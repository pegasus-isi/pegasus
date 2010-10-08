package edu.isi.pegasus.planner.dax;

/**
 *
 * @author gmehta
 */
public class Job extends AbstractJob {

    public Job(String id, String name) {
        this(id, null, name, null, null);
    }

    public Job(String id, String name, String label) {
        this(id, null, name, null, label);
    }

    public Job(String id, String namespace, String name, String version) {
        this(id, namespace, name, version, null);
    }

    public Job(String id, String namespace, String name, String version, String label) {
        super();
        checkID(id);

        // to decide whether to exit. Currently just logging error and proceeding.
        mId = id;
        mName = name;
        mNamespace = namespace;

        mVersion = version;
        mNodeLabel = label;
    }

    public String getNamespace() {
        return mNamespace;
    }

    public String getVersion() {
        return mVersion;
    }
}
