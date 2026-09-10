.. _cli-pegasus-transfer:

================
pegasus-transfer
================

Handles data transfers for Pegasus workflows.
   ::

      pegasus-transfer [-h]
                         [--file inputfile]
                         [--threads number_threads]
                         [--max-attempts attempts]
                         [--threads threads]
                         [--symlink]
                         [--debug]

Description
===========

**pegasus-transfer** takes a JSON defined list of urls, either through
stdin or with an input file, determines the correct tool to use for the
transfer and executes the transfer. Some of the protocols
pegasus-transfer can handle are GridFTP, SCP, SRM, Amazon S3, Google
Storage, XRootD, HTTP, Docker, Singularity, and local cp/symlinking.
Failed transfers are retried.

Note that pegasus-transfer is a tool mostly used internally in Pegasus
workflows, but the tool can be used stand alone as well.

Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**-f** *FILE*; \ **--file=FILE**
   File containing URL pairs to be transferred. If not given, list is
   read from stdin.

**-m** *MAX_ATTEMPTS*; \ **--max-attempts=MAX_ATTEMPTS**
   Number of attempts allowed for each transfer. Default is 3.

**-n** *THREADS*; \ **--threads=THREADS**
   Number of threads to process transfers. Default is 8. This option can
   also be set via the PEGASUS_TRANSFER_THREADS environment variable.
   The command line option takes precedence over the environment
   variable.

**-s**; \ **--symlink**
   Allow symlinking of file URLs. If the source and destination URLs
   chosen are both file URLs with the same site_label then the source
   file will be symlinked to the destination rather than being copied.

**-d**; \ **--debug**
   Enables debugging output.

Example
=======

::

   $ pegasus-transfer
   [
    { "type": "transfer",
      "id": 1,
      "src_urls": [ { "site_label": "web", "url": "http://pegasus.isi.edu" } ],
      "dest_urls": [ { "site_label": "local", "url": "file:///tmp/index.html" } ]
    }
   ]
   CTRL+D


Protocols Supported
===================

pegasus-transfer currently supports the following data transfer protocols:

Amazon S3 (native)
cp/symlinking (native)
Docker Pull
Globus Online
Google Storage
GridFTP
HPSS
HTTP/HTTPS (native)
iRODS
SCP
Singularity Library
SRM
StashCache
WebDAV (native)

Protocols marked "native" are implemented directly in pegasus-transfer
rather than by calling out to a separate client tool. Amazon S3 support
used to be provided by a separate **pegasus-s3** command-line tool;
that tool has been retired and its functionality — including the
``s3[s]://USER@SITE[/BUCKET[/KEY]]`` URL format and the credentials file
format described below — now lives directly in pegasus-transfer.

With the exception of Globus Online and HPSS, pegasus-transfer can handle
transfers between seemingly incompatible protocols by inserting a file://
intermediary. For example, if you ask pegasus-transfer for a transfer
between Docker and S3, it will be converted to two transfers, such that:

docker:// -> file://
file:// -> s3://


Credential Handling
===================

Credentials used for transfers can be specified with a combination of
site labels in the input JSON format and environment variables. For
example, give the following input file:

::

   [
    { "type": "transfer",
      "id": 1,
      "src_urls": [ { "site_label": "isi", "url": "gsiftp://workflow.isi.edu/data/file.dat" } ],
      "dest_urls": [ { "site_label": "tacc_stampede", "url": "gsiftp://gridftp.stampede.tacc.utexas.edu/scratch/file.dat" } ]
    }
   ]

pegasus-transfer will expect either one environment variable specifying
one credential to be used on both end of the connection
(X509_USER_PROXY), or two separate environment variables specifying two
different credentials to be used on the two ends of the connection. In
the latter case, the environment variables are derived from the site
labels. In the example above, the environment variables would be named
X509_USER_PROXY_isi and X509_USER_PROXY_tacc_stampede


Threading
=========

In order to speed up data transfers, pegasus-transfer will start a set
of transfers in parallel using threads.


Retries
=======

Failed transfers are retried, with an exponential backoff between the
tries. If there are a lot of transfers failing in on attempt,
pegasus-transfer might choose to short-circuit and fail early instead
of trying all transfers multiple times.


.. _pegasus-transfer-s3:

Amazon S3 Support
==================

pegasus-transfer includes native support for the Amazon S3 object
storage service and any other storage service that conforms to the
Amazon S3 API (such as Open Storage Network / OSN). This was previously
provided by a separate **pegasus-s3** command-line tool, which has since
been retired; the URL format and configuration file format below are
unchanged from that tool.

S3 URL Format
-------------

All URLs for objects stored in S3 should be specified in the following
format:

::

   s3[s]://USER@SITE[/BUCKET[/KEY]]

The protocol part can be *s3://* or *s3s://*. If *s3s://* is used, the
connection is forced to use SSL, overriding the setting in the
configuration file. If *s3://* is used, whether the connection uses SSL
is determined by the *endpoint* variable in the configuration for the
site.

The *USER@SITE* part is required, but the *BUCKET* and *KEY* parts may
be optional depending on the context. The *USER@SITE* portion is
referred to as the "identity", and the *SITE* portion is referred to as
the "site". Both the identity and the site are looked up in the
configuration file (see `S3 Configuration`_) to determine the
parameters to use when establishing a connection to the service. Note
that neither the *USER* nor the *SITE* portion of the URL have any
meaning outside of pegasus-transfer — they do not refer to real
usernames or hostnames, but are handles used to look up configuration
values in the configuration file.

The BUCKET portion of the URL is the part between the 3rd and 4th
slashes. The KEY portion is anything after the 4th slash and can
include additional slashes.

Some example URLs are:

::

   s3://ewa@amazon
   s3://juve@skynet/gideon.isi.edu
   s3://juve@magellan/pegasus-images/centos-5.5-x86_64-20101101.part.1
   s3s://ewa@amazon/pegasus-images/data.tar.gz

S3 Configuration
-----------------

Each user should specify a configuration file that pegasus-transfer
will use to look up connection parameters and authentication tokens for
S3 transfers.

**Search path.** pegasus-transfer looks in the following locations, in
order, to locate the configuration file: the ``PEGASUS_CREDENTIALS`` or
``PEGASUS_CREDENTIALS_<site_label>`` environment variable (see
`Credential Handling`_ above), or ``$HOME/.pegasus/credentials.conf`` by
default.

**File format.** The configuration file is in INI format with two kinds
of entries: a site entry, specifying the service endpoint for a storage
service, and an identity entry, specifying authentication credentials
for a user at a site.

::

   [amazon]
   endpoint = https://s3.amazonaws.com/

   [pegasus@amazon]
   access_key = 90c4143642cb097c88fe2ec66ce4ad4e
   secret_key = a0e3840e5baee6abb08be68e81674dca

.. warning::
  Access and secret keys should not be quoted.

The configuration file should be saved with limited permissions — only
the owner should be able to read from and write to it (0600 or 0400).
If the file has more liberal permissions, pegasus-transfer will fail
with an error message.

**Configuration variables:**

**endpoint** (site)
   The URL of the web service endpoint.

**region** (site)
   The AWS region for the endpoint, if applicable.

**batch_delete_size** (site)
   Number of keys deleted per batch request during a recursive S3
   remove. Defaults to ``1000``.

**access_key** (identity)
   The access key for the identity.

**secret_key** (identity)
   The secret key for the identity.

Preference of GFAL over GUC
===========================

JGlobus is no longer actively supported and is not in compliance RFC
2818. As a result cleanup jobs using pegasus-gridftp client would fail
against the servers supporting the strict mode. We have removed the
pegasus-gridftp client and now use gfal clients as globus-url-copy does
not support removes. If gfal is not available, globus-url-copy is used
for cleanup by writing out zero bytes files instead of removing them.

If you want to force globus-url-copy to be preferred over GFAL, set the
PEGASUS_FORCE_GUC=1 environment variable in the site catalog for the
sites you want the preference to be enforced. Please note that we expect
globus-url-copy support to be completely removed in future releases of
Pegasus due to the end of life of Globus Toolkit in 2018.


Author
======

Pegasus Team http://pegasus.isi.edu
