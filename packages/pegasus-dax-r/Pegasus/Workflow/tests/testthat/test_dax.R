library(pegasus)
context('Test DAX')

dax1 <- DAX(file='foo.dax', id='ID000001')
dax2file <- File('foo-2.dax')
dax2 <- DAX(dax2file, id='ID000002')
job1 <- Job("job-1", id='ID000003')

dax1 <- AddProfile(dax1, Profile(Pegasus.Namespace$ENV, 'PATH', '/bin'))
dax1 <- AddProfile(dax1, Profile(Pegasus.Namespace$CONDOR, 'universe', 'vanilla'))

i1 <- Invoke(Pegasus.When$AT_END, '/usr/bin/mail -s "job done" rafsilva@isi.edu')
i2 <- Invoke(Pegasus.When$ON_ERROR, '/usr/bin/update_db -failure')
dax1 <- AddInvoke(dax1, i1)
dax1 <- AddInvoke(dax1, i2)

dax <- Workflow('workflow-dax')
dax <- AddDAX(dax, dax1)
dax <- AddDAX(dax, dax2)
dax <- AddJob(dax, job1)

test_that('dax creation', {
  expect_true(Equals(dax1, dax1))
  expect_true(Equals(dax2, dax2))
  expect_false(Equals(dax1, dax2))
  expect_match(dax1$file$catalog.type$name, 'foo.dax')
  expect_match(dax2$file$catalog.type$name, 'foo-2.dax')
})

test_that('adding profiles', {
  expect_error(AddProfile(dax1, Profile(Pegasus.Namespace$ENV, 'PATH', '/bin')), 'Duplicate profile: /bin')
  expect_error(AddProfile(dax1, Profile(Pegasus.Namespace$CONDOR, 'universe', 'vanilla')), 'Duplicate profile: vanilla')
  expect_equal(length(dax1$abstract.job$profile.mixin$profiles), 2)
  expect_is(dax1$abstract.job$profile.mixin$profiles[[1]], 'Profile')
  expect_match(dax1$abstract.job$profile.mixin$profiles[[1]]$namespace, Pegasus.Namespace$ENV)
  expect_match(dax1$abstract.job$profile.mixin$profiles[[2]]$key, 'universe')
})

test_that('adding invokes', {
  expect_error(AddInvoke(dax1, i1), 'Duplicate Invoke')
  expect_error(AddInvoke(dax1, i2), 'Duplicate Invoke')
  expect_equal(length(dax1$abstract.job$invoke.mixin$invocations), 2)
  expect_is(dax1$abstract.job$invoke.mixin$invocations[[1]], 'Invoke')
  expect_match(dax1$abstract.job$invoke.mixin$invocations[[1]]$when, Pegasus.When$AT_END)
  expect_match(dax1$abstract.job$invoke.mixin$invocations[[2]]$what, '/usr/bin/update_db -failure')
})

test_that('manipulating daxs', {
  expect_equal(length(dax$jobs), 3)
  expect_true(HasJob(dax, dax1))
  expect_true(HasJob(dax, dax2))
  expect_error(AddDAX(dax, dax1), 'Duplicate job')
  expect_error(AddDAX(dax, dax2), 'Duplicate job')

  dax <- RemoveJob(dax, dax1)
  expect_equal(length(dax$jobs), 2)
  expect_false(HasJob(dax, dax1))
  expect_true(HasJob(dax, dax2))

  dax <- AddDAX(dax, dax1)
  dax <- ClearJobs(dax)
  expect_equal(length(dax$jobs), 0)
  expect_false(HasJob(dax, dax1))
  expect_false(HasJob(dax, dax2))
})

test_that('YAML generation', {
  expect_equal(length(capture.output(WriteYAML(dax, stdout()))), 17)
})
