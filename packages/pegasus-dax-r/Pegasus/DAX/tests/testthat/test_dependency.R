library('dax3')
context('Test Dependency')

job1 <- Job('j-1', id='ID000001')
dax1 <- DAX(File('fdax.txt'), id='ID000002')
dag1 <- DAG(File('fdag.txt'), id='ID000003')

dep1 <- Dependency(job1, dax1)
dep2 <- Dependency(job1, dag1)

dax <- ADAG('workflow-dax')
dax <- AddJob(dax, job1)
dax <- AddDAX(dax, dax1)
dax <- AddDAG(dax, dag1)
dax <- AddDependency(dax, dep1)
dax <- AddDependency(dax, dep2)

test_that('dependency creation', {
  expect_true(Equals(dep1, dep1))
  expect_true(Equals(dep2, dep2))
  expect_false(Equals(dep1, dep2))
  expect_match(dep1$parent, 'ID000001')
  expect_match(dep2$parent, 'ID000001')
  expect_match(dep1$child, 'ID000002')
  expect_match(dep2$child, 'ID000003')
  expect_error(Dependency(NULL, NULL), "Invalid parent")
  expect_error(Dependency(job1, NULL), "Invalid child")
})

test_that('manipulating dependencies', {
  expect_equal(length(dax$dependencies), 2)
  expect_true(HasDependency(dax, dep1))
  expect_true(HasDependency(dax, dep2))
  expect_error(AddDependency(dax, dep1), 'Duplicate dependency')
  expect_error(AddDependency(dax, dep2), 'Duplicate dependency')

  dax <- RemoveDependency(dax, dep1)
  expect_equal(length(dax$dependencies), 1)
  expect_false(HasDependency(dax, dep1))
  expect_true(HasDependency(dax, dep2))

  dax <- AddDependency(dax, dep1)
  dax <- ClearDependencies(dax)
  expect_equal(length(dax$dependencies), 0)
  expect_false(HasDependency(dax, dep1))
  expect_false(HasDependency(dax, dep2))
})

test_that('XML generation', {
  expect_equal(length(capture.output(WriteXML(dax, stdout()))), 20)
})
