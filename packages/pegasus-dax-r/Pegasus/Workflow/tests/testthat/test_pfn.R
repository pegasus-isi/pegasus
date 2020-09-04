library(pegasus)
context('Test PFN')

p1 <- PFN(url = 'file:///p1.txt')
p2 <- PFN(url = 'file:///p1.txt', site = 'site-name')

p1 <- AddProfile(p1, Profile(Pegasus.Namespace$ENV, 'PATH', '/bin'))
p1 <- AddProfile(p1, Profile(Pegasus.Namespace$CONDOR, 'universe', 'vanilla'))

test_that('PFN creation', {
  expect_match(p1$url, 'file:///p1.txt')
  expect_match(p1$site, 'local')
  expect_match(p2$url, 'file:///p1.txt')
  expect_match(p2$site, 'site-name')
  expect_true(Equals(p1, p1))
  expect_true(Equals(p2, p2))
  expect_false(Equals(p1, p2))
  expect_error(PFN(NULL), 'Invalid url')
  expect_error(PFN('file:///p1.txt', NULL), 'Invalid site')
})

test_that('adding profiles', {
  expect_equal(length(p1$profile.mixin$profiles), 2)
  expect_error(AddProfile(p1, Profile(Pegasus.Namespace$ENV, 'PATH', '/bin')), 'Duplicate profile: /bin')
  expect_error(AddProfile(p1, Profile(Pegasus.Namespace$CONDOR, 'universe', 'vanilla')), 'Duplicate profile: vanilla')
})
