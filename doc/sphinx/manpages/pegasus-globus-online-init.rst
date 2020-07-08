==========================
pegasus-globus-online-init
==========================

1
pegasus-globus-online-init
Initializes OAuth tokens for Globus Online authentication.
   ::

      pegasus-globus-online-init  [-h]
                                    [--permanent]



Description
===========

**pegasus-globus-online-init** initializes OAuth tokens, to be used with
Globus Online transfers. It redirects the user to globus website, in
order to authorize Pegasus wms to perform transfers with the user’s
Globus account. By default this tool requests tokens that cannot be
refreshed and could potentially expire within a couple of days. In order
to provide pegasus with refreshable tokens please use --permanent
option. The acquired tokens are placed in globus.conf inside .pegasus
folder of the user’s home directory.

Note this tool should be used before starting a workflow that relies on
Globus Online transfers, unless the user has initialized the tokens with
another way or has acquired refreshable tokens previously.



Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**--permanent**
   Requests a refresh token that can be used indefinetely. Access can be
   revoked from globus web interface (manage consents)



Author
======

Pegasus Team http://pegasus.isi.edu
