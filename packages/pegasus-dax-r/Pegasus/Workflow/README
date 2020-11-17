API for generating Pegasus Abstract Workflows

The classes in this module can be used to generate workflows that can be
read by Pegasus.

The official Workflow schema is here: http://pegasus.isi.edu/schema/
Here is an example showing how to create the diamond abstract workflow using this API:

library(pegasus)

# Create an abstract workflow
diamond <- Workflow("diamond")

# Add some metadata
diamond <- Metadata(diamond, "name", "diamond")
diamond <- Metadata(diamond, "createdby", "Rafael Ferreira da Silva")

# Add input file to the DAX-level replica catalog
a <- File("f.a")
a <- AddPFN(a, PFN("gsiftp://site.com/inputs/f.a","site"))
a <- Metadata(a, "size", "1024")
diamond <- AddFile(diamond, a)

# Add executables to the DAX-level replica catalog
e_preprocess <- Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64")
e_preprocess <- Metadata(e_preprocess, "size", "2048")
e_preprocess <- AddPFN(e_preprocess, PFN("gsiftp://site.com/bin/preprocess","site"))
diamond <- AddExecutable(diamond, e_preprocess)

e_findrange <- Executable(namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64")
e_findrange <- AddPFN(e_findrange, PFN("gsiftp://site.com/bin/findrange","site"))
diamond <- AddExecutable(diamond, e_findrange)

e_analyze <- Executable(namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64")
e_analyze <- AddPFN(e_analyze, PFN("gsiftp://site.com/bin/analyze","site"))
diamond <- AddExecutable(diamond, e_analyze)

# Add a preprocess job
preprocess <- Job(e.preprocess)
preprocess <- Metadata(preprocess, "time", "60")
b1 <- File("f.b1")
b2 <- File("f.b2")
preprocess <- AddArguments(preprocess, list("-a","preprocess","-T","3","-i",a,"-o",b1,b2))
preprocess <- Uses(preprocess, a, link=Pegasus.Link$INPUT)
preprocess <- Uses(preprocess, b1, link=Pegasus.Link$OUTPUT, transfer=TRUE)
preprocess <- Uses(preprocess, b2, link=Pegasus.Link$OUTPUT, transfer=TRUE)
diamond <- AddJob(diamond, preprocess)

# Add left Findrange job
frl <- Job(e.findrange)
frl <- Metadata(frl, "time", "60")
c1 <- File("f.c1")
frl <- AddArguments(frl, list("-a","findrange","-T","3","-i",b1,"-o",c1))
frl <- Uses(frl, b1, link=Pegasus.Link$INPUT)
frl <- Uses(frl, c1, link=Pegasus.Link$OUTPUT, transfer=TRUE)
diamond <- AddJob(diamond, frl)

# Add right Findrange job
frr <- Job(e.findrange)
frr <- Metadata(frr, "time", "60")
c2 <- File("f.c2")
frr <- AddArguments(frr, list("-a","findrange","-T","3","-i",b2,"-o",c2))
frr <- Uses(frr, b2, link=Pegasus.Link$INPUT)
frr <- Uses(frr, c2, link=Pegasus.Link$OUTPUT, transfer=TRUE)
diamond <- AddJob(diamond, frr)

# Add Analyze job
analyze <- Job(e.analyze)
analyze <- Metadata(analyze, "time", "60")
d <- File("f.d")
analyze <- AddArguments(analyze, list("-a","analyze","-T","3","-i",c1,c2,"-o",d))
analyze <- Uses(analyze, c1, link=Pegasus.Link$INPUT)
analyze <- Uses(analyze, c2, link=Pegasus.Link$INPUT)
analyze <- Uses(analyze, d, link=Pegasus.Link$OUTPUT, transfer=TRUE)
diamond <- AddJob(diamond, analyze)

# Add dependencies
diamond <- Depends(diamond, parent=preprocess, child=frl)
diamond <- Depends(diamond, parent=preprocess, child=frr)
diamond <- Depends(diamond, parent=frl, child=analyze)
diamond <- Depends(diamond, parent=frr, child=analyze)

# Get generated diamond dax
WriteYAML(diamond, stdout())
