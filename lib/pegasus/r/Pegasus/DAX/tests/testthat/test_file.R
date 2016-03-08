library('dax3')
context('Test File')

f1 <- File('f1.txt')
f2 <- File('f2.txt')

f1 <- AddPFN(f1, PFN('file:///f1.txt', site='local'))
f1 <- AddPFN(f1, PFN('file:///f1.txt', site='site-external'))

f1 <- AddProfile(f1, Profile(DAX3.Namespace$ENV, 'PATH', '/bin'))
f1 <- AddProfile(f1, Profile(DAX3.Namespace$CONDOR, 'universe', 'vanilla'))

f1 <- Metadata(f1, 'size', '1024')
f1 <- Metadata(f1, 'createdby', 'Rafael Ferreira da Silva')

dax <- ADAG('workflow-dax')
dax <- AddFile(dax, f1)
dax <- AddFile(dax, f2)

test_that('file creation', {
  expect_error(File(), 'argument "name" is missing, with no default')
  expect_true(Equals(f1, f1))
  expect_true(Equals(f2, f2))
  expect_false(Equals(f1, f2))
  expect_match(f1$catalog.type$name, 'f1.txt')
  expect_match(f2$catalog.type$name, 'f2.txt')
})

test_that('adding PFNs', {
  expect_error(AddPFN(f1, PFN('file:///f1.txt', site='local')), 'Duplicate PFN: local')
  expect_error(AddPFN(f1, PFN('file:///f1.txt', site='site-external')), 'Duplicate PFN: site-external')
  expect_equal(length(f1$catalog.type$pfn.mixin$pfns), 2)
  expect_is(f1$catalog.type$pfn.mixin$pfns[[1]], "PFN")
  expect_match(f1$catalog.type$pfn.mixin$pfns[[1]]$url, 'file:///f1.txt')
})

test_that('adding profiles', {
  expect_error(AddProfile(f1, Profile(DAX3.Namespace$ENV, 'PATH', '/bin')), 'Duplicate profile: /bin')
  expect_error(AddProfile(f1, Profile(DAX3.Namespace$CONDOR, 'universe', 'vanilla')), 'Duplicate profile: vanilla')
  expect_equal(length(f1$catalog.type$profile.mixin$profiles), 2)
  expect_is(f1$catalog.type$profile.mixin$profiles[[1]], 'Profile')
  expect_match(f1$catalog.type$profile.mixin$profiles[[1]]$namespace, DAX3.Namespace$ENV)
  expect_match(f1$catalog.type$profile.mixin$profiles[[2]]$key, 'universe')
})

test_that('adding metadata', {
  expect_error(Metadata(f1, "size", "1024"), 'Duplicate metadata: size')
  expect_error(Metadata(f1, "createdby", "Rafael Ferreira da Silva"), 'Duplicate metadata: Rafael Ferreira da Silva')
  expect_equal(length(f1$catalog.type$metadata.mixin$metadata.l), 2)
  expect_is(f1$catalog.type$metadata.mixin$metadata.l[[1]], 'Metadata')
  expect_match(f1$catalog.type$metadata.mixin$metadata.l[[2]]$key, 'createdby')
  expect_match(f1$catalog.type$metadata.mixin$metadata.l[[1]]$value, '1024')
})

test_that('manipulating files', {
  expect_equal(length(dax$files), 2)
  expect_true(HasFile(dax, f1))
  expect_true(HasFile(dax, f2))
  expect_error(AddFile(dax, f1), 'Duplicate file: f1.txt')
  expect_error(AddFile(dax, f2), 'Duplicate file: f2.txt')

  dax <- RemoveFile(dax, f1)
  expect_equal(length(dax$files), 1)
  expect_false(HasFile(dax, f1))
  expect_true(HasFile(dax, f2))

  dax <- AddFile(dax, f1)
  dax <- ClearFiles(dax)
  expect_equal(length(dax$files), 0)
  expect_false(HasFile(dax, f1))
  expect_false(HasFile(dax, f2))
})

test_that('XML generation', {
  expect_equal(length(capture.output(WriteXML(dax, stdout()))), 17)
})
