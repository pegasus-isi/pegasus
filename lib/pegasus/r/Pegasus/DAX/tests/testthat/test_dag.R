library('dax3')
context('Test DAG')

dag1 <- DAG(file='foo.dag', id='ID000001')
dag2file <- File('foo-2.dag')
dag2 <- DAG(dag2file, id='ID000002')
job1 <- Job("job-1", id='ID000003')

dag1 <- AddProfile(dag1, Profile(DAX3.Namespace$ENV, 'PATH', '/bin'))
dag1 <- AddProfile(dag1, Profile(DAX3.Namespace$CONDOR, 'universe', 'vanilla'))

i1 <- Invoke(DAX3.When$AT_END, '/usr/bin/mail -s "job done" rafsilva@isi.edu')
i2 <- Invoke(DAX3.When$ON_ERROR, '/usr/bin/update_db -failure')
dag1 <- AddInvoke(dag1, i1)
dag1 <- AddInvoke(dag1, i2)

dax <- ADAG('workflow-dax')
dax <- AddDAG(dax, dag1)
dax <- AddDAG(dax, dag2)
dax <- AddJob(dax, job1)


test_that('DAG creation', {
  expect_true(Equals(dag1, dag1))
  expect_true(Equals(dag2, dag2))
  expect_false(Equals(dag1, dag2))
  expect_match(dag1$file$catalog.type$name, 'foo.dag')
  expect_match(dag2$file$catalog.type$name, 'foo-2.dag')
})

test_that('adding profiles', {
  expect_error(AddProfile(dag1, Profile(DAX3.Namespace$ENV, 'PATH', '/bin')), 'Duplicate profile: /bin')
  expect_error(AddProfile(dag1, Profile(DAX3.Namespace$CONDOR, 'universe', 'vanilla')), 'Duplicate profile: vanilla')
  expect_equal(length(dag1$abstract.job$profile.mixin$profiles), 2)
  expect_is(dag1$abstract.job$profile.mixin$profiles[[1]], 'Profile')
  expect_match(dag1$abstract.job$profile.mixin$profiles[[1]]$namespace, DAX3.Namespace$ENV)
  expect_match(dag1$abstract.job$profile.mixin$profiles[[2]]$key, 'universe')
})

test_that('adding invokes', {
  expect_error(AddInvoke(dag1, i1), 'Duplicate Invoke')
  expect_error(AddInvoke(dag1, i2), 'Duplicate Invoke')
  expect_equal(length(dag1$abstract.job$invoke.mixin$invocations), 2)
  expect_is(dag1$abstract.job$invoke.mixin$invocations[[1]], 'Invoke')
  expect_match(dag1$abstract.job$invoke.mixin$invocations[[1]]$when, DAX3.When$AT_END)
  expect_match(dag1$abstract.job$invoke.mixin$invocations[[2]]$what, '/usr/bin/update_db -failure')
})

test_that('manipulating DAGs', {
  expect_equal(length(dax$jobs), 3)
  expect_true(HasJob(dax, dag1))
  expect_true(HasJob(dax, dag2))
  expect_error(AddDAG(dax, dag1), 'Duplicate job')
  expect_error(AddDAG(dax, dag2), 'Duplicate job')

  dax <- RemoveJob(dax, dag1)
  expect_equal(length(dax$jobs), 2)
  expect_false(HasJob(dax, dag1))
  expect_true(HasJob(dax, dag2))

  dax <- AddDAG(dax, dag1)
  dax <- ClearJobs(dax)
  expect_equal(length(dax$jobs), 0)
  expect_false(HasJob(dax, dag1))
  expect_false(HasJob(dax, dag2))
})

test_that('XML generation', {
  expect_equal(length(capture.output(WriteXML(dax, stdout()))), 17)
})
