TEST DESCRIPTION
- clustering is turned on
- worker package transfer is not explicity set. instead it is only
transferred for condorpool that is run in condorio mode.
- CCG site is sharedfs
- condorpool is condorio (default configuration)
- in pegasusrc pegasus.data.configuration is not set.


PURPOSE
- to execute workflow in multisite configuration with each site having
a different data configuration.

Associated JIRA Item
- https://jira.isi.edu/browse/PM-810
