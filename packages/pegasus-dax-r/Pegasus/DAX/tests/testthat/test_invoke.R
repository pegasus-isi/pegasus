library('dax3')
context('Test Invoke')

i1 <- Invoke(DAX3.When$AT_END, '/usr/bin/mail -s "job done" rafsilva@isi.edu')
i2 <- Invoke(DAX3.When$ON_ERROR, '/usr/bin/update_db -failure')

e1 <- Executable('grep')
e1 <- AddInvoke(e1, i1)
e1 <- AddInvoke(e1, i2)

d1 <- DAG(File('a.txt'))
d1 <- AddInvoke(d1, i1)
d1 <- AddInvoke(d1, i2)

test_that('Invoke creation', {
  expect_match(i1$when, 'at_end')
  expect_match(i1$when, DAX3.When$AT_END)
  expect_match(i1$what, '/usr/bin/mail -s "job done" rafsilva@isi.edu')
  expect_match(i2$when, 'on_error')
  expect_match(i2$when, DAX3.When$ON_ERROR)
  expect_match(i2$what, '/usr/bin/update_db -failure')
  expect_true(Equals(i1, i1))
  expect_true(Equals(i2, i2))
  expect_false(Equals(i1, i2))
  expect_error(Invoke(NULL), 'Invalid when')
  expect_error(Invoke(DAX3.When$ON_SUCCESS, NULL), 'Invalid what')
})

test_that('adding invokes', {
  expect_equal(length(e1$invoke.mixin$invocations), 2)
  expect_error(AddInvoke(e1, i1), 'Duplicate Invoke')
  expect_error(AddInvoke(e1, i2), 'Duplicate Invoke')
  expect_equal(length(d1$abstract.job$invoke.mixin$invocations), 2)
  expect_error(AddInvoke(d1, i1), 'Duplicate Invoke')
  expect_error(AddInvoke(d1, i2), 'Duplicate Invoke')
})
