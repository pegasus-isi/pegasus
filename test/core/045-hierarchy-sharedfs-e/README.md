A hierarchal workflow running in nonsharedfs mode
Tests out 2 main things
- data dependency between a sub workflow job and a parent compute job
- tests inplace cleanup for the above setup. the cleanup job deleting file f.a should be a child of the sub workflow job also


