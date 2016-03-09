#!/usr/bin/Rscript

if (is.element("dax3", installed.packages()[,1])) {
  require(dax3)
} else {
  stop("The R DAX Generator API is not installed.\nPlease refer to 'https://pegasus.isi.edu/documentation/dax_generator_api.php' on how to install it.")
}

# Reading arguments
args <- commandArgs(trailingOnly = TRUE)

# Create a DAX
diamond <- ADAG("diamond")

# Add input file to the DAX-level replica catalog
a <- File("f.a")
a <- AddPFN(a, PFN(paste("file://", getwd(), "/f.a", sep=""), "local"))
diamond <- AddFile(diamond, a)

# Add executables to the DAX-level replica catalog
e_preprocess <- Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64", installed=TRUE)
e_preprocess <- AddPFN(e_preprocess, PFN(paste("file://", args[1], "/bin/pegasus-keg", sep=""), "TestCluster"))
diamond <- AddExecutable(diamond, e_preprocess)

e_findrange <- Executable(namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64", installed=TRUE)
e_findrange <- AddPFN(e_findrange, PFN(paste("file://", args[1], "/bin/pegasus-keg", sep=""), "TestCluster"))
diamond <- AddExecutable(diamond, e_findrange)

e_analyze <- Executable(namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64", installed=TRUE)
e_analyze <- AddPFN(e_analyze, PFN(paste("file://", args[1], "/bin/pegasus-keg", sep=""), "TestCluster"))
diamond <- AddExecutable(diamond, e_analyze)

# Add a preprocess job
preprocess <- Job(e_preprocess)
b1 <- File("f.b1")
b2 <- File("f.b2")
preprocess <- AddArguments(preprocess, list("-a preprocess", "-T60", "-i", a, "-o", b1, b2))
preprocess <- Uses(preprocess, a, link=DAX3.Link$INPUT)
preprocess <- Uses(preprocess, b1, link=DAX3.Link$OUTPUT)
preprocess <- Uses(preprocess, b2, link=DAX3.Link$OUTPUT)
diamond <- AddJob(diamond, preprocess)

# Add left Findrange job
frl <- Job(e_findrange)
c1 <- File("f.c1")
frl <- AddArguments(frl, list("-a findrange", "-T60", "-i", b1, "-o", c1))
frl <- Uses(frl, b1, link=DAX3.Link$INPUT)
frl <- Uses(frl, c1, link=DAX3.Link$OUTPUT)
diamond <- AddJob(diamond, frl)

# Add right Findrange job
frr <- Job(e_findrange)
c2 <- File("f.c2")
frr <- AddArguments(frr, list("-a findrange", "-T60", "-i", b2, "-o", c2))
frr <- Uses(frr, b2, link=DAX3.Link$INPUT)
frr <- Uses(frr, c2, link=DAX3.Link$OUTPUT)
diamond <- AddJob(diamond, frr)

# Add Analyze job
analyze <- Job(e_analyze)
d <- File("f.d")
analyze <- AddArguments(analyze, list("-a analyze", "-T60", "-i", c1, c2, "-o", d))
analyze <- Uses(analyze, c1, link=DAX3.Link$INPUT)
analyze <- Uses(analyze, c2, link=DAX3.Link$INPUT)
analyze <- Uses(analyze, d, link=DAX3.Link$OUTPUT, register=TRUE)
diamond <- AddJob(diamond, analyze)

# Add dependencies
diamond <- Depends(diamond, parent=preprocess, child=frl)
diamond <- Depends(diamond, parent=preprocess, child=frr)
diamond <- Depends(diamond, parent=frl, child=analyze)
diamond <- Depends(diamond, parent=frr, child=analyze)

# Get generated diamond dax
WriteXML(diamond, stdout())
