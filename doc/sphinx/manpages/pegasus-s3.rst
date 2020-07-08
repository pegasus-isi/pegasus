==========
pegasus-s3
==========

Upload, download, delete objects in Amazon S3.
   ::

      pegasus-s3 help
      pegasus-s3 ls [options] URL                
      pegasus-s3 mkdir URL
      pegasus-s3 rm [options] URL
      pegasus-s3 put [options] FILE URL 
      pegasus-s3 get [options] URL [FILE]
      pegasus-s3 cp [options] SRC… DEST



Description
===========

**pegasus-s3** is a client for the Amazon S3 object storage service and
any other storage services that conform to the Amazon S3 API, such as
Eucalyptus Walrus. Note that this tool is mainly used internally by 
:ref:`cli-pegasus-transfer` and it is recommended to use Amazon's
`AWS Command Line Interface`_.

.. _AWS Command Line Interface: https://aws.amazon.com/cli/


Options
=======



Global Options
--------------

**-h**; \ **--help**
   Show help message for subcommand and exit

**-d**; \ **--debug**
   Turn on debugging

**-v**; \ **--verbose**
   Show progress messages

**-C** *FILE*; \ **--conf**\ =\ *FILE*
   Path to configuration file


ls Options
----------

**-l**; \ **--long**
   Use long listing format that includes size, etc.

**-H**; \ **--human-sized**
   Use human readable sizes

rm Options
----------

**-f**; \ **--force**
   Ignore nonexistent keys

**-F**; \ **--file**
   File containing a list of URLs to delete 

put Options
-----------

**-b**; \ **--create-bucket**
   Create the destination bucket if it does not already exist

**-f**; \ **--force**
   Overwrite key if it already exists

cp Options
----------

**-c**; \ **--create-dest**
   Create the destination bucket if it does not exist.

**-f**; \ **--force**
   If DEST exists, then overwrite it.



Subcommands
===========

**pegasus-s3** has several subcommands for different storage service
operations.

**help**
   The help subcommand lists all available subcommands.

**ls**
   The **ls** subcommand lists the contents of a URL. If the URL does
   not contain a bucket, then all the buckets owned by the user are
   listed. If the URL contains a bucket, but no key, then all the keys
   in the bucket are listed. If the URL contains a bucket and a key,
   then all keys in the bucket that begin with the specified key are
   listed.

**mkdir**
   The **mkdir** subcommand creates one or more buckets.

**rm**
   The **rm** subcommand deletes one or more keys from the storage
   service.

**put**
   The **put** subcommand stores the file specified by FILE in the
   storage service under the bucket and key specified by URL. If the URL
   contains a bucket, but not a key, then the file name is used as the
   key. If URL ends with a "/", then the file name is appended to the
   URL to create the key name (e.g. ``pegasus-s3 put foo
   s3://u@h/bucket/key`` will create a key called "key", while
   ``pegasus-s3 put foo s3://u@h/bucket/key/`` will create a key called
   ``key/foo``. 

**get**
   The **get** subcommand retrieves an object from the storage service
   identified by URL and stores it in the file specified by FILE. If
   FILE is not specified, then the part of the key after the last "/" is
   used as the file name, and the result is placed in the
   current working directory. 

**cp**
   The **cp** subcommand copies keys on the server. Keys cannot be
   copied between accounts.


URL Format
==========

All URLs for objects stored in S3 should be specified in the following
format:

::

   s3[s]://USER@SITE[/BUCKET[/KEY]]

The protocol part can be *s3://* or *s3s://*. If *s3s://* is used, then
**pegasus-s3** will force the connection to use SSL and override the
setting in the configuration file. If s3:// is used, then whether the
connection uses SSL or not is determined by the value of the *endpoint*
variable in the configuration for the site.

The *USER@SITE* part is required, but the *BUCKET* and *KEY* parts may
be optional depending on the context.

The *USER@SITE* portion is referred to as the “identity”, and the *SITE*
portion is referred to as the “site”. Both the identity and the site are
looked up in the configuration file (see
:ref:`S3_CONFIGURATION`) to determine the parameters to
use when establishing a connection to the service. The site portion is
used to find the host and port, whether to use SSL, and other things.
The identity portion is used to determine which authentication tokens to
use. This format is designed to enable users to easily use multiple
services with multiple authentication tokens. Note that neither the
*USER* nor the *SITE* portion of the URL have any meaning outside of
**pegasus-s3**. They do not refer to real usernames or hostnames, but
are rather handles used to look up configuration values in the
configuration file.

The BUCKET portion of the URL is the part between the 3rd and 4th
slashes. Buckets are part of a global namespace that is shared with
other users of the storage service. As such, they should be unique.

The KEY portion of the URL is anything after the 4th slash. Keys can
include slashes, but S3-like storage services do not have the concept of
a directory like regular file systems. Instead, keys are treated like
opaque identifiers for individual objects. So, for example, the keys
*a/b* and *a/c* have a common prefix, but cannot be said to be in the
same *directory*.

Some example URLs are:

::

   s3://ewa@amazon
   s3://juve@skynet/gideon.isi.edu
   s3://juve@magellan/pegasus-images/centos-5.5-x86_64-20101101.part.1
   s3s://ewa@amazon/pegasus-images/data.tar.gz

.. _S3_CONFIGURATION:

Configuration
=============

Each user should specify a configuration file that **pegasus-s3** will
use to look up connection parameters and authentication tokens.



Search Path
-----------

This client will look in the following locations, in order, to locate
the user’s configuration file:

1. The -C/--conf argument

2. The S3CFG environment variable

3. $HOME/.pegasus/s3cfg

4. $HOME/.s3cfg

If it does not find the configuration file in one of these locations it
will fail with an error. The ``$HOME/.s3cfg`` location is only supported for
backward-compatibility. ``$HOME/.pegasus/s3cfg`` should be used instead.



Configuration File Format
-------------------------

The configuration file is in INI format and contains two types of
entries.

The first type of entry is a site entry, which specifies the
configuration for a storage service. This entry specifies the service
endpoint that **pegasus-s3** should connect to for the site, and some
optional features that the site may support. Here is an example of a
site entry for Amazon S3:

::

   [amazon]
   endpoint = http://s3.amazonaws.com/

The other type of entry is an identity entry, which specifies the
authentication information for a user at a particular site. Here is an
example of an identity entry:

::

   [pegasus@amazon]
   access_key = 90c4143642cb097c88fe2ec66ce4ad4e
   secret_key = a0e3840e5baee6abb08be68e81674dca

It is important to note that user names and site names used are only
logical—they do not correspond to actual hostnames or usernames, but are
simply used as a convenient way to refer to the services and identities
used by the client.

The configuration file should be saved with limited permissions. Only
the owner of the file should be able to read from it and write to it
(i.e. it should have permissions of 0600 or 0400). If the file has more
liberal permissions, then **pegasus-s3** will fail with an error
message. The purpose of this is to prevent the authentication tokens
stored in the configuration file from being accessed by other users.



Configuration Variables
-----------------------

**endpoint** (site)
   The URL of the web service endpoint.

**max_object_size** (site)
   The maximum size of an object in GB (default: 5GB)

**multipart_uploads** (site)
   Does the service support multipart uploads (True/False, default:
   False)

**ranged_downloads** (site)
   Does the service support ranged downloads? (True/False, default:
   False)

**access_key** (identity)
   The access key for the identity

**secret_key** (identity)
   The secret key for the identity



Example Configuration
---------------------

This is an example configuration that specifies a two sites (amazon and
magellan) and three identities (``pegasus@amazon``,\ ``juve@magellan``,
and ``voeckler@magellan``). For the amazon site the maximum object size
is 5TB, and the site supports both multipart uploads and ranged
downloads, so both uploads and downloads can be done in parallel.

::

   [amazon]
   endpoint = https://s3.amazonaws.com/
   max_object_size = 5120
   multipart_uploads = True
   ranged_downloads = True

   [pegasus@amazon]
   access_key = 90c4143642cb097c88fe2ec66ce4ad4e
   secret_key = a0e3840e5baee6abb08be68e81674dca

   [magellan]
   # NERSC Magellan is a Eucalyptus site. It doesn't support multipart uploads,
   # or ranged downloads (the defaults), and the maximum object size is 5GB
   # (also the default)
   endpoint = https://128.55.69.235:8773/services/Walrus

   [juve@magellan]
   access_key = quwefahsdpfwlkewqjsdoijldsdf
   secret_key = asdfa9wejalsdjfljasldjfasdfa

   [voeckler@magellan]
   # Each site can have multiple associated identities
   access_key = asdkfaweasdfbaeiwhkjfbaqwhei
   secret_key = asdhfuinakwjelfuhalsdflahsdl



Example
=======

List all buckets owned by identity *user@amazon*:

::

   $ pegasus-s3 ls s3://user@amazon

List the contents of bucket *bar* for identity *user@amazon*:

::

   $ pegasus-s3 ls s3://user@amazon/bar

List all objects in bucket *bar* that start with *hello*:

::

   $ pegasus-s3 ls s3://user@amazon/bar/hello

Create a bucket called *mybucket* for identity *user@amazon*:

::

   $ pegasus-s3 mkdir s3://user@amazon/mybucket

Upload a file *foo* to bucket *bar*:

::

   $ pegasus-s3 put foo s3://user@amazon/bar/foo

Download an object *foo* in bucket *bar*:

::

   $ pegasus-s3 get s3://user@amazon/bar/foo foo


Return Value
============

**pegasus-s3** returns a zero exist status if the operation is
successful. A non-zero exit status is returned in case of failure.



Author
======

| Gideon Juve ``<gideon@isi.edu>``
| Ryan Tanaka ``<tanaka@isi.edu>``

Pegasus Team https://pegasus.isi.edu
