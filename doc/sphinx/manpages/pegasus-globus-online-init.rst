.. _cli-pegasus-globus-online-init:

==========================
pegasus-globus-online-init
==========================

Initializes OAuth tokens for Globus Transfer authentication.
::

      pegasus-globus-online-init  [-h]
                                  [--permanent]
                                  [--collections]
                                  [--endpoints]
                                  [--domains]



Description
===========

**pegasus-globus-online-init** initializes OAuth tokens, to be used with
Globus transfers. It redirects the user to the Globus website, in
order to authorize Pegasus WMS to perform transfers with the user’s
Globus account. By default this tool requests tokens that cannot be
refreshed and could potentially expire within a couple of days. In order
to provide pegasus with refreshable tokens please use --permanent
option. In case of domain authenticated tokens, the lifespan of the
refreshable token is dictated by the policies of domain.
The acquired tokens are placed in globus.conf inside .pegasus
folder of the user’s home directory.

Note this tool should be used before starting a workflow that relies on
Globus transfers, unless the user has initialized the tokens with
another way or has acquired refreshable tokens previously.



Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**-p**; \ **--permanent**
   Requests a refresh token that can be used indefinetely. Access can be
   revoked from globus web interface (manage consents).

**-c**; \ **--collections**
   A list of collection uuids that require data_access consent to move data to
   and from them. Access can be revoked from the globus web interface.

**-e**; \ **--endpoints**
   A list of endpoint uuids that require manage_collections consent to create
   collections in them. Access can be revoked from the globus web interface.

**-d**; \ **--domains**
   A list of domain requirements that must be satisfied by the valid identities 
   of the user under globus auth. The token generated will be granted the domain.
