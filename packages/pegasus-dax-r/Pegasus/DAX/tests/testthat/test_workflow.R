library(dax3)
context("Test Workflow")

# Create a DAX
diamond <- ADAG("diamond")

# Add some metadata
diamond <- Metadata(diamond, "name", "diamond")
diamond <- Metadata(diamond, "createdby", "Rafael Ferreira da Silva")

expect_equal(length(diamond$metadata.mixin$metadata.l), 2)
expect_match(diamond$metadata.mixin$metadata.l[[1]]$key, "name")
expect_match(diamond$metadata.mixin$metadata.l[[2]]$value, "Rafael Ferreira da Silva")

# Add input file to the DAX-level replica catalog
a <- File("f.a")
a <- AddPFN(a, PFN("gsiftp://site.com/inputs/f.a","site"))
a <- Metadata(a, "size", "1024")
diamond <- AddFile(diamond, a)

expect_equal(length(diamond$files), 1)
expect_match(diamond$files[[1]]$catalog.type$name, "f.a")
expect_match(diamond$files[[1]]$catalog.type$pfn.mixin$pfns[[1]]$url, "gsiftp://site.com/inputs/f.a")
expect_match(diamond$files[[1]]$catalog.type$metadata.mixin$metadata.l[[1]]$value, "1024")

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

expect_equal(length(diamond$executables), 3)
expect_match(diamond$executables[[1]]$catalog.type$name, "preprocess")
expect_match(diamond$executables[[3]]$catalog.type$name, "analyze")
expect_match(diamond$executables[[1]]$catalog.type$metadata.mixin$metadata.l[[1]]$key, "size")
expect_match(diamond$executables[[2]]$catalog.type$pfn.mixin$pfns[[1]]$url, "gsiftp://site.com/bin/findrange")
expect_match(diamond$executables[[1]]$version, "4.0")
expect_match(diamond$executables[[2]]$arch, "x86_64")
expect_match(diamond$executables[[3]]$os, "linux")

# Add a preprocess job
preprocess <- Job(e_preprocess)
preprocess <- Metadata(preprocess, "time", "60")
b1 <- File("f.b1")
b2 <- File("f.b2")
preprocess <- AddArguments(preprocess, list("-a preprocess","-T60","-i",a,"-o",b1,b2))
preprocess <- Uses(preprocess, a, link=DAX3.Link$INPUT)
preprocess <- Uses(preprocess, b1, link=DAX3.Link$OUTPUT, transfer=TRUE)
preprocess <- Uses(preprocess, b2, link=DAX3.Link$OUTPUT, transfer=TRUE)
diamond <- AddJob(diamond, preprocess)

expect_match(diamond$jobs$ID0000001$name, "preprocess")
expect_match(diamond$jobs$ID0000001$abstract.job$metadata.mixin$metadata.l[[1]]$key, "time")
expect_equal(length(diamond$jobs$ID0000001$abstract.job$arguments), 13)
expect_equal(length(diamond$jobs$ID0000001$abstract.job$use.mixin$used), 3)
expect_is(diamond$jobs$ID0000001$abstract.job$use.mixin$used[[1]]$name, "File")
expect_match(diamond$jobs$ID0000001$abstract.job$use.mixin$used[[1]]$name$catalog.type$name, "f.a")
expect_equal(diamond$jobs$ID0000001$abstract.job$use.mixin$used[[1]]$link, "input")
expect_true(diamond$jobs$ID0000001$abstract.job$use.mixin$used[[2]]$transfer)

# Add left Findrange job
frl <- Job(e_findrange)
frl <- Metadata(frl, "time", "60")
c1 <- File("f.c1")
frl <- AddArguments(frl, list("-a findrange","-T60","-i",b1,"-o",c1))
frl <- Uses(frl, b1, link=DAX3.Link$INPUT)
frl <- Uses(frl, c1, link=DAX3.Link$OUTPUT, transfer=TRUE)
diamond <- AddJob(diamond, frl)

expect_is(diamond$jobs$ID0000002, "Job")
expect_match(diamond$jobs$ID0000002$name, "findrange")
expect_is(diamond$jobs$ID0000002$abstract.job$arguments[[7]], "File")
expect_equal(diamond$jobs$ID0000002$abstract.job$arguments[[11]], c1)
expect_equal(diamond$jobs$ID0000002$abstract.job$use.mixin$used[[2]]$link, "output")

# Add right Findrange job
frr <- Job(e_findrange)
frr <- Metadata(frr, "time", "60")
c2 <- File("f.c2")
frr <- AddArguments(frr, list("-a findrange","-T60","-i",b2,"-o",c2))
frr <- Uses(frr, b2, link=DAX3.Link$INPUT)
frr <- Uses(frr, c2, link=DAX3.Link$OUTPUT, transfer=TRUE)
diamond <- AddJob(diamond, frr)

expect_match(diamond$jobs$ID0000003$name, "findrange")
expect_equal(diamond$jobs$ID0000003$abstract.job$arguments[[7]], b2)
expect_equal(diamond$jobs$ID0000003$abstract.job$arguments[[11]], c2)

# Add Analyze job
analyze <- Job(e_analyze)
analyze <- Metadata(analyze, "time", "60")
d <- File("f.d")
analyze <- AddArguments(analyze, list("-a analyze","-T60","-i",c1,c2,"-o",d))
analyze <- Uses(analyze, c1, link=DAX3.Link$INPUT)
analyze <- Uses(analyze, c2, link=DAX3.Link$INPUT)
analyze <- Uses(analyze, d, link=DAX3.Link$OUTPUT, transfer=TRUE)
diamond <- AddJob(diamond, analyze)

expect_match(diamond$jobs$ID0000004$name, "analyze")
expect_equal(diamond$jobs$ID0000004$abstract.job$arguments[[7]], c1)
expect_equal(diamond$jobs$ID0000004$abstract.job$arguments[[9]], c2)
expect_equal(diamond$jobs$ID0000004$abstract.job$arguments[[13]], d)
expect_equal(length(diamond$jobs), 4)

# Add dependencies
diamond <- Depends(diamond, parent=preprocess, child=frl)
diamond <- Depends(diamond, parent=preprocess, child=frr)
diamond <- Depends(diamond, parent=frl, child=analyze)
diamond <- Depends(diamond, parent=frr, child=analyze)

expect_equal(length(diamond$dependencies), 4)
expect_match(diamond$dependencies[[1]]$parent, "ID0000001")
expect_match(diamond$dependencies[[1]]$child, "ID0000002")
expect_match(diamond$dependencies[[2]]$parent, "ID0000001")
expect_match(diamond$dependencies[[2]]$child, "ID0000003")
expect_match(diamond$dependencies[[3]]$parent, "ID0000002")
expect_match(diamond$dependencies[[3]]$child, "ID0000004")
expect_match(diamond$dependencies[[4]]$parent, "ID0000003")
expect_match(diamond$dependencies[[4]]$child, "ID0000004")

# Get generated diamond dax
co <- capture.output(WriteXML(diamond, stdout()))
expect_equal(length(co), 69)
