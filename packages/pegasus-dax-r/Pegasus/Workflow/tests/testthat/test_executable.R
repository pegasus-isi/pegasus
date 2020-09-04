library(pegasus)
context('Test Executable')

e1 <- Executable(namespace='os',name='grep',version='2.3',arch=Pegasus.Arch$X86,os=Pegasus.OS$LINUX)
e2 <- Executable(namespace='os',name='grep',version='2.3',arch=Pegasus.Arch$X86)

e1 <- AddPFN(e1, PFN('file:///grep', site='local'))
e1 <- AddPFN(e1, PFN('file:///grep', site='site-external'))

e1 <- AddProfile(e1, Profile(Pegasus.Namespace$ENV, 'PATH', '/bin'))
e1 <- AddProfile(e1, Profile(Pegasus.Namespace$CONDOR, 'universe', 'vanilla'))

e1 <- Metadata(e1, 'size', '1024')
e1 <- Metadata(e1, 'createdby', 'Rafael Ferreira da Silva')

dax <- Workflow('workflow-dax')
dax <- AddExecutable(dax, e1)
dax <- AddExecutable(dax, e2)

test_that('executable creation', {
  expect_true(Equals(e1, e1))
  expect_true(Equals(e2, e2))
  expect_false(Equals(e1, e2))
  expect_match(e1$catalog.type$name, 'grep')
  expect_match(e2$catalog.type$name, 'grep')
})

test_that('adding PFNs', {
  expect_error(AddPFN(e1, PFN('file:///grep', site='local')), 'Duplicate PFN: local')
  expect_error(AddPFN(e1, PFN('file:///grep', site='site-external')), 'Duplicate PFN: site-external')
  expect_equal(length(e1$catalog.type$pfn.mixin$pfns), 2)
  expect_is(e1$catalog.type$pfn.mixin$pfns[[1]], 'PFN')
  expect_match(e1$catalog.type$pfn.mixin$pfns[[1]]$url, 'file:///grep')
})

test_that('adding profiles', {
  expect_error(AddProfile(e1, Profile(Pegasus.Namespace$ENV, 'PATH', '/bin')), 'Duplicate profile: /bin')
  expect_error(AddProfile(e1, Profile(Pegasus.Namespace$CONDOR, 'universe', 'vanilla')), 'Duplicate profile: vanilla')
  expect_equal(length(e1$catalog.type$profile.mixin$profiles), 2)
  expect_is(e1$catalog.type$profile.mixin$profiles[[1]], 'Profile')
  expect_match(e1$catalog.type$profile.mixin$profiles[[1]]$namespace, Pegasus.Namespace$ENV)
  expect_match(e1$catalog.type$profile.mixin$profiles[[2]]$key, 'universe')
})

test_that('adding metadata', {
  expect_error(Metadata(e1, 'size', '1024'), 'Duplicate metadata: size')
  expect_error(Metadata(e1, 'createdby', 'Rafael Ferreira da Silva'), 'Duplicate metadata: Rafael Ferreira da Silva')
  expect_equal(length(e1$catalog.type$metadata.mixin$metadata.l), 2)
  expect_is(e1$catalog.type$metadata.mixin$metadata.l[[1]], 'Metadata')
  expect_match(e1$catalog.type$metadata.mixin$metadata.l[[2]]$key, 'createdby')
  expect_match(e1$catalog.type$metadata.mixin$metadata.l[[1]]$value, '1024')
})

test_that('manipulating executables', {
  expect_equal(length(dax$executables), 2)
  expect_true(HasExecutable(dax, e1))
  expect_true(HasExecutable(dax, e2))
  expect_error(AddExecutable(dax, e1), 'Duplicate executable: grep')
  expect_error(AddExecutable(dax, e2), 'Duplicate executable: grep')

  dax <- RemoveExecutable(dax, e1)
  expect_equal(length(dax$executables), 1)
  expect_false(HasExecutable(dax, e1))
  expect_true(HasExecutable(dax, e2))

  dax <- AddExecutable(dax, e1)
  dax <- ClearExecutables(dax)
  expect_equal(length(dax$executables), 0)
  expect_false(HasExecutable(dax, e1))
  expect_false(HasExecutable(dax, e2))
})

test_that('YAML generation', {
  expect_equal(length(capture.output(WriteYAML(dax, stdout()))), 7)
})
