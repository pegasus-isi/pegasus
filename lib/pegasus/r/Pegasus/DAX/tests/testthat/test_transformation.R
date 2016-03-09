library('dax3')
context('Test Transformation')

# Declarations
e1 <- Executable(namespace='montage',name='mProjectPP',version='3.0')
e2 <- Executable(namespace='montage',name='mFitplane',version='3.0')
e3 <- Executable(namespace='montage',name='mDiffFit',version='3.0')
f1 <- File('f1.txt')
i1 <- Invoke(DAX3.When$AT_END, '/usr/bin/mail -s "job done" rafsilva@isi.edu')
i2 <- Invoke(DAX3.When$ON_ERROR, '/usr/bin/update_db -failure')

x1 <- Transformation(e3)
x1 <- Uses(x1, e1)
x1 <- Uses(x1, e2)
x1 <- Uses(x1, f1)
x1 <- Metadata(x1, 'has', 'A file and 2 executables')
x1 <- Metadata(x1, 'createdby', 'Rafael Ferreira da Silva')
x1 <- AddInvoke(x1, i1)
x1 <- AddInvoke(x1, i2)

f2 <- File("jbsim.conf")
e4 <- Executable(namespace="scec",name="jbsim")

x2 <- Transformation(f2)
x2 <- Uses(x2, f2)

dax <- ADAG('workflow-dax')
dax <- AddTransformation(dax, x1)
dax <- AddTransformation(dax, x2)

# Tests
test_that('transformation creation', {
  expect_true(Equals(x1, x1))
  expect_match(x1$name, 'mDiffFit')
  expect_match(x1$namespace, 'montage')
  expect_match(x1$version, '3.0')
})

test_that('adding uses', {
  expect_error(Uses(x1, e1), 'Duplicate Use:')
  expect_error(Uses(x1, e2), 'Duplicate Use:')
  expect_error(Uses(x1, f1), 'Duplicate Use:')
  expect_equal(length(x1$use.mixin$used), 3)
  expect_is(x1$use.mixin$used[[1]]$name, 'Executable')
  expect_is(x1$use.mixin$used[[3]]$name, 'File')
})

test_that('adding invocations', {
  expect_error(AddInvoke(x1, i1), 'Duplicate Invoke')
  expect_error(AddInvoke(x1, i2), 'Duplicate Invoke')
  expect_equal(length(x1$invoke.mixin$invocations), 2)
  expect_is(x1$invoke.mixin$invocations[[1]], 'Invoke')
  expect_match(x1$invoke.mixin$invocations[[1]]$when, DAX3.When$AT_END)
  expect_match(x1$invoke.mixin$invocations[[2]]$what, '/usr/bin/update_db -failure')
})

test_that('adding metadata', {
  expect_error(Metadata(x1, 'has', 'A file and 2 executables'), 'Duplicate metadata: has')
  expect_error(Metadata(x1, 'createdby', 'Rafael Ferreira da Silva'), 'Duplicate metadata: Rafael Ferreira da Silva')
  expect_equal(length(x1$metadata.mixin$metadata.l), 2)
  expect_is(x1$metadata.mixin$metadata.l[[1]], 'Metadata')
  expect_match(x1$metadata.mixin$metadata.l[[2]]$key, 'createdby')
  expect_match(x1$metadata.mixin$metadata.l[[1]]$value, 'A file and 2 executables')
})

test_that('manipulating transformations', {
  expect_equal(length(dax$transformations), 2)
  expect_true(HasTransformation(dax, x1))
  expect_true(HasTransformation(dax, x2))
  expect_error(AddTransformation(dax, x1), 'Duplicate transformation')
  expect_error(AddTransformation(dax, x2), 'Duplicate transformation')

  dax <- RemoveTransformation(dax, x1)
  expect_equal(length(dax$transformations), 1)
  expect_false(HasTransformation(dax, x1))
  expect_true(HasTransformation(dax, x2))

  dax <- AddTransformation(dax, x1)
  dax <- ClearTransformations(dax)
  expect_equal(length(dax$transformations), 0)
  expect_false(HasTransformation(dax, x1))
  expect_false(HasTransformation(dax, x2))
})

test_that('xml generation', {
  expect_equal(length(capture.output(WriteXML(dax, stdout()))), 20)
})
