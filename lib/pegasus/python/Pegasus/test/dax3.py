# This is a sort-of kitchen sink dax generator for validating that
# the API produces valid XML for all elements according to the
# new DAX 3.3 schema. It tries to generate XML to cover every part
# of the schema.

from Pegasus.DAX3 import *


def main():
    # Create a DAX
    diamond = ADAG("diamond", index=1, count=10)
    diamond.metadata("key", "value")
    diamond.metadata(key="foo", value="bar")
    diamond.invoke(what="what", when="when")

    # Add input file to the DAX-level replica catalog
    a = File("f.a")
    a.metadata("key", "value")
    a.profile("pegasus", "foobar", "true")
    a.PFN("gsiftp://site.com/inputs/f.a", "site")
    diamond.addFile(a)

    cfg = File("config.ini")
    cfg.metadata("size", "10")
    diamond.addFile(cfg)

    # Add executables to the DAX-level replica catalog
    e_preprocess = Executable(
        namespace="diamond",
        name="preprocess",
        version="4.0",
        os="linux",
        osrelease="5",
        glibc="3.3",
        arch="x86_64",
        installed=True,
        osversion="2.6",
    )
    e_preprocess.profile("pegasus", "barfoo", "false")
    e_preprocess.metadata("size", 100)
    pfn = PFN("gsiftp://site.com/bin/preprocess", "site")
    pfn.profile("pegasus", "baz", "abcd")
    e_preprocess.addPFN(pfn)
    e_preprocess.invoke(what="what", when="when")
    diamond.addExecutable(e_preprocess)

    e_findrange = Executable(
        namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64"
    )
    e_findrange.addPFN(PFN("gsiftp://site.com/bin/findrange", "site"))
    diamond.addExecutable(e_findrange)

    e_analyze = Executable(
        namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64"
    )
    e_analyze.addPFN(PFN("gsiftp://site.com/bin/analyze", "site"))
    diamond.addExecutable(e_analyze)

    # Add transformations to the DAX-level transformation catalog
    t_preprocess = Transformation(e_preprocess)
    t_preprocess.metadata("key", "value")
    t_preprocess.invoke(what="what", when="when")
    t_preprocess.uses(cfg)
    diamond.addTransformation(t_preprocess)

    t_findrange = Transformation(e_findrange)
    t_findrange.uses(cfg)
    diamond.addTransformation(t_findrange)

    t_analyze = Transformation(e_analyze)
    t_analyze.uses(cfg)
    diamond.addTransformation(t_analyze)

    # Add a preprocess job
    preprocess = Job(t_preprocess)
    preprocess.metadata("key", "value")
    b1 = File("f.b1")
    b1.metadata("key", "value")
    b2 = File("f.b2")
    b2.metadata("key", "value")
    preprocess.addArguments("-a preprocess", "-T60", "-i", a, "-o", b1, b2)
    preprocess.profile("pegasus", "site", "local")
    preprocess.setStdin(File("stdin"))
    preprocess.setStdout(File("stdout"))
    preprocess.setStderr(File("stderr"))
    preprocess.uses(a, link=Link.INPUT, optional=True)
    preprocess.uses(b1, link=Link.OUTPUT, transfer=True, optional=True)
    preprocess.uses(b2, link=Link.OUTPUT, transfer=True, register=True)
    preprocess.uses(e_preprocess)
    preprocess.invoke(when="when", what="what")
    diamond.addJob(preprocess)

    # Add left Findrange job
    frl = Job(t_findrange, node_label="foo")
    c1 = File("f.c1")
    frl.addArguments("-a findrange", "-T60", "-i", b1, "-o", c1)
    diamond.addJob(frl)

    # Add right Findrange job
    frr = Job(t_findrange)
    c2 = File("f.c2")
    frr.addArguments("-a findrange", "-T60", "-i", b2, "-o", c2)
    frr.uses(b2, link=Link.INPUT)
    frr.uses(c2, link=Link.OUTPUT, transfer=True)
    diamond.addJob(frr)

    # Add Analyze job
    analyze = Job(t_analyze)
    d = File("f.d")
    analyze.addArguments("-a analyze", "-T60", "-i", c1, c2, "-o", d)
    analyze.uses(c1, link=Link.INPUT)
    analyze.uses(c2, link=Link.INPUT)
    analyze.uses(d, link=Link.OUTPUT, transfer=True, register=True)
    diamond.addJob(analyze)

    dax = DAX("file.dax", node_label="apple")
    dax.addArguments("-Dpegasus.properties=foobar")
    dax.metadata("key", "value")
    dax.profile("pegasus", "site", "local")
    dax.setStdin(File("stdin"))
    dax.setStdout(File("stdout"))
    dax.setStderr(File("stderr"))
    dax.uses(a, link=Link.INPUT, optional=True)
    dax.uses(b1, link=Link.OUTPUT, transfer=True, optional=True)
    dax.uses(b2, link=Link.OUTPUT, transfer=True, register=True)
    dax.uses(e_preprocess)
    dax.invoke(when="when", what="what")
    diamond.addJob(dax)

    dag = DAG("file.dag", node_label="pear")
    dag.addArguments("-Dpegasus.properties=foobar")
    dag.metadata("key", "value")
    dag.profile("pegasus", "site", "local")
    dag.setStdin(File("stdin"))
    dag.setStdout(File("stdout"))
    dag.setStderr(File("stderr"))
    dag.uses(a, link=Link.INPUT, optional=True)
    dag.uses(b1, link=Link.OUTPUT, transfer=True, optional=True)
    dag.uses(b2, link=Link.OUTPUT, transfer=True, register=True)
    dag.uses(e_preprocess)
    dag.invoke(when="when", what="what")
    diamond.addDAG(dag)

    # Add dependencies
    diamond.depends(parent=preprocess, child=frl, edge_label="foobar")
    diamond.depends(parent=preprocess, child=frr)
    diamond.depends(parent=frl, child=analyze)
    diamond.depends(parent=frr, child=analyze)

    # Get generated diamond dax
    import sys

    diamond.writeXML(sys.stdout)


if __name__ == "__main__":
    main()
