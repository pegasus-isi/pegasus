#
#  Copyright 2010-2016 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import sys
import os
import math
import stat
import traceback
import threading
import Queue
import fnmatch
import re
import time
from optparse import OptionParser
from urlparse import urlsplit
from ConfigParser import ConfigParser

try:
    import boto
    import boto.exception
    import boto.s3.connection
    from boto.s3.bucket import Bucket
    from boto.s3.key import Key
except ImportError as e:
    sys.stderr.write("ERROR: Unable to load boto library: %s\n" % e)
    exit(1)

# do not use http proxies for S3
if "http_proxy" in os.environ:
    del os.environ['http_proxy']

# Don't let apple hijack our cacerts
os.environ["OPENSSL_X509_TEA_DISABLE"] = "1"

# The multipart upload feature we require was introduced in 2.2.2
boto_version = tuple(int(x) for x in boto.__version__.split("."))
if boto_version < (2,2,2):
    sys.stderr.write("Requires boto 2.2.2 or later, not %s\n" % boto.__version__)
    exit(1)

# set boto config options
try:
    boto.config.add_section('Boto')
except:
    pass
boto.config.set('Boto','http_socket_timeout','60')

COMMANDS = {
    'ls': 'List the contents of a bucket',
    'mkdir': 'Create a bucket in S3',
    'rmdir': 'Delete a bucket from S3',
    'rm': 'Delete a key from S3',
    'put': 'Upload a key to S3 from a file',
    'get': 'Download a key from S3 to a file',
    'lsup': 'List multipart uploads',
    'rmup': 'Cancel multipart uploads',
    'cp': 'Copy keys remotely',
    'help': 'Print this message'
}

KB = 1024
MB = 1024*KB
GB = 1024*MB
TB = 1024*GB

LOCATIONS = {
    "s3.amazonaws.com": "",
    "s3-us-west-1.amazonaws.com": "us-west-1",
    "s3-us-west-2.amazonaws.com": "us-west-2",
    "s3-eu-west-1.amazonaws.com": "EU",
    "s3-ap-southeast-1.amazonaws.com": "ap-southeast-1",
    "s3-ap-southeast-2.amazonaws.com": "ap-southeast-2",
    "s3-ap-northeast-1.amazonaws.com": "ap-northeast-1",
    "s3-sa-east-1.amazonaws.com": "sa-east-1"
}

DEFAULT_CONFIG = {
    "max_object_size": str(5),
    "multipart_uploads": str(False),
    "ranged_downloads": str(False),
    "batch_delete": str(True),
    "batch_delete_size": str(1000)
}

DEBUG = False
VERBOSE = False

class WorkThread(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.exception = None
        self.daemon = True

    def run(self):
        try:
            # Just keep grabbing work units and
            # executing them until there are no
            # more to process, then exit
            while True:
                fn = self.queue.get(False)
                fn()
        except Queue.Empty:
            return
        except Exception as e:
            traceback.print_exc()
            self.exception = e

def debug(message):
    if DEBUG:
        sys.stderr.write("%s\n" % message)

def info(message):
    if VERBOSE:
        sys.stdout.write("%s\n" % message)

def warn(message):
    sys.stderr.write("WARNING: %s\n" % message)

def fix_file(url):
    if url.startswith("file://"):
        url = url.replace("file:","")
    return url

def has_wildcards(string):
    if string is None:
        return False
    wildcards = "*?[]"
    for c in wildcards:
        if c in string:
            return True
    return False

def help(*args):
    sys.stderr.write("Usage: %s COMMAND\n\n" % os.path.basename(sys.argv[0]))
    sys.stderr.write("Commands:\n")
    for cmd in COMMANDS:
        sys.stderr.write("    %-8s%s\n" % (cmd, COMMANDS[cmd]))

def option_parser(usage):
    command = os.path.basename(sys.argv[0])

    parser = OptionParser(usage="usage: %s %s" % (command, usage))
    parser.add_option("-d", "--debug", dest="debug", action="store_true",
        default=False, help="Turn on debugging")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
        default=False, help="Show progress messages")
    parser.add_option("-C", "--conf", dest="config",
        metavar="FILE", default=None,
        help="Path to configuration file")

    # Add a hook so we can handle global arguments
    fn = parser.parse_args
    def parse(*args, **kwargs):
        options, args = fn(*args, **kwargs)

        if options.debug:
            boto.set_stream_logger("boto")
            global DEBUG
            DEBUG = True

        if options.verbose:
            global VERBOSE
            VERBOSE = True

        return options, args
    parser.parse_args = parse

    return parser

def get_config(options):
    S3CFG = os.getenv("S3CFG", None)
    if options.config:
        # Command-line overrides everything
        cfg = options.config
    elif S3CFG is not None:
        # Environment variable overrides defaults
        cfg = S3CFG
    else:
        # New default
        new_default = os.path.expanduser("~/.pegasus/s3cfg")
        if os.path.isfile(new_default):
            cfg = new_default
        else:
            # If the new default doesn't exist, try the old default
            cfg = os.path.expanduser("~/.s3cfg")

    if not os.path.isfile(cfg):
        raise Exception("Config file not found")

    debug("Found config file: %s" % cfg)

    # Make sure nobody else can read the file
    mode = os.stat(cfg).st_mode
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise Exception("Permissions of config file %s are too liberal" % cfg)

    config = ConfigParser(DEFAULT_CONFIG)
    config.read(cfg)

    return config

def parse_endpoint(uri):
    result = urlsplit(uri)

    kwargs = {
        'is_secure': result.scheme=='https',
        'host': result.hostname,
        'port': result.port,
        'path': result.path
    }

    location = LOCATIONS.get(result.hostname, '')

    return kwargs, location

def get_connection(config, uri):
    if not config.has_section(uri.site):
        raise Exception("Config file has no section for site '%s'" % uri.site)

    if not config.has_section(uri.ident):
        raise Exception("Config file has no section for identity '%s'" % uri.ident)

    endpoint = config.get(uri.site,"endpoint")
    kwargs, location = parse_endpoint(endpoint)

    kwargs['aws_access_key_id'] = config.get(uri.ident,"access_key")
    kwargs['aws_secret_access_key'] = config.get(uri.ident,"secret_key")
    kwargs['calling_format'] = boto.s3.connection.OrdinaryCallingFormat()

    # If the URI is s3s, then override the config
    if uri.secure:
        kwargs['validate_certs'] = True
        kwargs['is_secure'] = True

    conn = boto.s3.connection.S3Connection(**kwargs)
    conn.location = location

    return conn

def read_command_file(path):
    tokenizer = re.compile(r"\s+")
    f = open(path, "r")
    try:
        for line in f:
            line = line.strip()
            if len(line) == 0:
                continue
            if line.startswith("#"):
                continue
            yield tokenizer.split(line)
    finally:
        f.close()

class S3URI:
    def __init__(self, user, site, bucket=None, key=None, secure=False):
        self.user = user
        self.site = site
        self.ident = "%s@%s" % (user, site)
        self.bucket = bucket
        self.key = key
        self.secure = secure

    def __repr__(self):
        if self.secure:
            uri = "s3s://%s" % self.ident
        else:
            uri = "s3://%s" % self.ident
        if self.bucket is not None:
            uri += "/%s" % self.bucket
        if self.key is not None:
            uri += "/%s" % self.key
        return uri

def parse_uri(uri):
    "Parse S3 uri into an S3URI object"

    # The only valid schemes are s3s:// and s3://
    if uri.startswith("s3s://"):
        secure = True
    elif uri.startswith("s3://"):
        secure = False
    else:
        raise Exception("Invalid URL scheme: %s" % (uri))

    # Need to do this because urlparse does not recognize
    # custom URI schemes. Replace our scheme with http.
    # The actual scheme used isn't important as long as
    # urlsplit recognizes it.
    if secure:
        http = uri.replace("s3s://","http://")
    else:
        http = uri.replace("s3://","http://")
    result = urlsplit(http)

    # The user should not be specifying a query part unless
    # they are trying to use the ? wildcard. If they do use
    # the ? wildcard, then urlsplit thinks it is the query
    # separator. In that case, we put the path and query
    # back together.
    if '?' in uri:
        path = "?".join([result.path, result.query]).strip()
    else:
        path = result.path.strip()

    # The path should be empty, /BUCKET or /BUCKET/KEY
    if path.startswith("/"):
        path = path[1:]
    if len(path) == 0:
        bucket = None
        key = None
    else:
        comp = path.split('/',1)
        bucket = comp[0]
        if len(comp) == 1:
            key = None
        elif comp[1] == '':
            key = None
        else:
            key = comp[1]

    # We require the username part
    user = result.username
    if user is None:
        raise Exception("User missing from URL: %s" % uri)

    if result.port is None:
        site = result.hostname
    else:
        site = "%s:%s" % (result.hostname, result.port)

    return S3URI(user, site, bucket, key, secure)

def ls(args):
    parser = option_parser("ls URL...")

    parser.add_option("-l", "--long", dest="long_format", action="store_true",
        default=False, help="Use long listing format")
    parser.add_option("-H", "--human-sized", dest="human_sized", action="store_true",
        default=False, help="Use human readable sizes")

    options, args = parser.parse_args(args)

    if len(args) == 0:
        parser.error("Specify a URL")

    config = get_config(options)

    items = []
    for uri in args:
        items.append(parse_uri(uri))

    def human_sized(sz):
        if sz > TB:
            return "%6.1fT" % (sz / (1.0*TB))
        elif sz > GB:
            return "%6.1fG" % (sz / (1.0*GB))
        elif sz > MB:
            return "%6.1fM" % (sz / (1.0*MB))
        elif sz > KB:
            return "%6.1fK" % (sz / (1.0*KB))
        else:
            return "%6dB" % sz

    def print_key(key):
        if options.long_format:
            if options.human_sized:
                size = human_sized(key.size)
            else:
                size = "%13d" % key.size
            name = key.name
            modified = key.last_modified
            owner = key.owner.display_name
            storage_class = key.storage_class
            if storage_class.startswith("REDUCED"):
                storage_class = "REDUCED"
            sys.stdout.write("\t%15s %s %s %8s %s\n" % (owner, size, modified, storage_class, name))
        else:
            sys.stdout.write("\t%s\n" % key.name)

    def print_bucket(bucket):
        if options.long_format:
            # Also list the location of each bucket
            loc = bucket.get_location()
            if not loc:
                loc = "-"
            sys.stdout.write("\t%-50s%s\n" % (bucket.name, loc))
        else:
            sys.stdout.write("\t%s\n" % bucket.name)

    for uri in items:
        conn = get_connection(config, uri)

        bucket = uri.bucket
        key = uri.key

        sys.stdout.write("%s\n" % uri)
        if bucket is None:
            buckets = conn.get_all_buckets()
            for bucket in buckets:
                print_bucket(bucket)
        else:
            b = conn.get_bucket(uri.bucket)
            if has_wildcards(uri.key):
                for o in b.list():
                    if fnmatch.fnmatch(o.name, uri.key):
                        print_key(o)
            else:
                for o in b.list(prefix=uri.key):
                    # For some reason, Walrus sometimes returns a Prefix object
                    if isinstance(o, boto.s3.prefix.Prefix):
                        continue
                    print_key(o)

def cp(args):
    parser = option_parser("cp SRC[...] DEST")

    parser.add_option("-c", "--create-dest", dest="create", action="store_true",
        default=False, help="Create destination bucket if it does not exist")
    parser.add_option("-r", "--recursive", dest="recurse", action="store_true",
        default=False, help="If SRC is a bucket, copy all its keys to DEST")
    parser.add_option("-f", "--force", dest="force", action="store_true",
        default=False, help="If DEST key exists, then overwrite it")

    options, args = parser.parse_args(args)

    if len(args) < 2:
        parser.error("Specify a SRC and DEST")

    config = get_config(options)

    items = [parse_uri(uri) for uri in args]

    # The last one is the DEST
    dest = items[-1]
    # Everything else is a SRC
    srcs = items[0:-1]

    # If there is more than one source, then the destination must be
    # a bucket and not a bucket+key.
    if len(srcs) > 1 and dest.key is not None:
        raise Exception("Destination must be a bucket if there are multiple sources")

    # Validate all the URI pairs
    for src in srcs:
        # The source URI must have a key unless the user specified -r
        if src.key is None and not options.recurse:
            raise Exception("Source URL does not contain a key "
                            "(see -r): %s" % src)

        # Each source must have the same identity as the destination. 
        # Copying from one account to another, or one region to another,
        # is not allowed.
        if src.ident != dest.ident:
            raise Exception("Identities for source and destination "
                            "do not match: %s -> %s" % (src,dest))

    conn = get_connection(config, dest)
    destbucket = conn.lookup(dest.bucket)
    if destbucket is None:
        if options.create:
            info("Creating destination bucket %s" % dest.bucket)
            destbucket = conn.create_bucket(dest.bucket, location=conn.location)
        else:
            raise Exception("Destination bucket %s does not exist "
                            "(see -c)" % dest.bucket)

    # Does the destination site support multipart uploads?
    multipart_uploads = config.getboolean(dest.site, "multipart_uploads")

    for src in srcs:
        srcbucket = conn.get_bucket(src.bucket)

        # If there is no src key, then copy all of the keys from
        # the src bucket. We already checked for -r above.
        if src.key is None:
            srckeys = srcbucket.list()
            info("Copying %d keys from %s to %s" % (len(srckeys), src, dest))
        else:
            k = srcbucket.get_key(src.key)
            if k is None:
                raise Exception("Source key '%s' does not exist" % src.key)
            srckeys = [k]

        # It doesn't make sense to copy several keys into one destination
        if len(srckeys) > 1 and dest.key:
            raise Exception("Cannot copy %d keys to one destination key" % len(srckeys))

        for srckey in srckeys:
            # If the destination key is not specified, then just use
            # the same key name as the source
            destkeyname = dest.key or srckey

            # If the user did not specify force, then check to make sure
            # the destination key does not exist
            if not options.force:
                info("Checking for existence of %s" % destkeyname)
                if destbucket.get_key(destkeyname) is not None:
                    raise Exception("Destination key %s exists "
                                    "(see -f)" % destkeyname)

            info("Copying %s/%s to %s/%s" % (src.bucket, srckey.name, dest.bucket, destkeyname))

            start_time = time.time()

            # If the source is larger than 5GB we need to use multipart uploads
            if multipart_uploads and srckey.size > 5*GB:
                info("Object is larger than 5GB: Using multipart uploads")
                i = 1
                start = 0
                parts = math.ceil(srckey.size / (1.0*GB))
                mp = destbucket.initiate_multipart_upload(destkeyname)
                while start < srckey.size:
                    end = min(start+GB-1, srckey.size-1)
                    info("Copying part %d of %d (%d-%d)" % (i, parts, start, end))
                    mp.copy_part_from_key(src.bucket, srckey.name, i, start, end)
                    i += 1
                    start = end + 1
                mp.complete_upload()
            else:
                destbucket.copy_key(new_key_name = destkeyname,
                                    src_bucket_name = src.bucket,
                                    src_key_name = srckey.name)

            end_time = time.time()

            size = srckey.size / 1024.0
            elapsed = end_time - start_time
            rate = size / elapsed

            info("Copied %0.1f KB in %0.6f seconds: %0.2f KB/s" % (size, elapsed, rate))

def mkdir(args):
    parser = option_parser("mkdir URL...")
    options, args = parser.parse_args(args)

    if len(args) == 0:
        parser.error("Specify URL")

    buckets = []
    for arg in args:
        uri = parse_uri(arg)
        if uri.bucket is None:
            raise Exception("URL for mkdir must contain a bucket: %s" % arg)
        if uri.key is not None:
            raise Exception("URL for mkdir cannot contain a key: %s" % arg)
        buckets.append(uri)

    config = get_config(options)

    for uri in buckets:
        info("Creating %s" % uri)
        conn = get_connection(config, uri)
        try:
            conn.create_bucket(uri.bucket, location=conn.location)
        except Exception as e:
            if hasattr(e, "error_message") and \
               "bucket succeeded and you already own it" in e.error_message:
                continue
            raise


def rmdir(args):
    parser = option_parser("rmdir URL...")
    options, args = parser.parse_args(args)

    if len(args) == 0:
        parser.error("Specify URL")

    buckets = []
    for arg in args:
        uri = parse_uri(arg)
        if uri.bucket is None:
            raise Exception("URL for rmdir must contain a bucket: %s" % arg)
        if uri.key is not None:
            raise Exception("URL for rmdir cannot contain a key: %s" % arg)
        buckets.append(uri)

    config = get_config(options)

    for uri in buckets:
        info("Removing bucket %s" % uri)
        conn = get_connection(config, uri)
        conn.delete_bucket(uri.bucket)

def rm(args):
    parser = option_parser("rm URL...")
    parser.add_option("-f", "--force", dest="force", action="store_true",
        default=False, help="Ignore nonexistent keys")
    parser.add_option("-F", "--file", dest="file", action="store",
        default=None, help="File containing a list of URLs to delete")
    options, args = parser.parse_args(args)

    if len(args) == 0 and not options.file:
        parser.error("Specify URL")

    if options.file:
        for rec in read_command_file(options.file):
            if len(rec) != 1:
                raise Exception("Invalid record: %s" % rec)
            args.append(rec[0])

    buckets = {}
    for arg in args:
        uri = parse_uri(arg)
        if uri.bucket is None:
            raise Exception("URL for rm must contain a bucket: %s" % arg)
        if uri.key is None:
            raise Exception("URL for rm must contain a key: %s" % arg)

        bid = "%s/%s" % (uri.ident, uri.bucket)
        buri = S3URI(uri.user, uri.site, uri.bucket, uri.secure)

        if bid not in buckets:
            buckets[bid] = (buri, [])
        buckets[bid][1].append(uri)

    config = get_config(options)

    for bucket in buckets:

        # Connect to the bucket
        debug("Deleting keys from bucket %s" % bucket)
        uri, keys = buckets[bucket]
        conn = get_connection(config, uri)
        b = Bucket(connection=conn, name=uri.bucket)

        # Get a final list of all the keys, resolving wildcards as necessary
        bucket_contents = None
        keys_to_delete = set()
        for key in keys:
            key_name = key.key

            if has_wildcards(key_name):

                # If we haven't yet queried the bucket, then do so now
                # so that we can match the wildcards
                if bucket_contents is None:
                    bucket_contents = b.list()

                # Collect all the keys that match
                for k in bucket_contents:
                    if fnmatch.fnmatch(k.name, key_name):
                        keys_to_delete.add(k.name)

            else:
                keys_to_delete.add(key_name)

        info("Deleting %d keys" % len(keys_to_delete))

        batch_delete = config.getboolean(uri.site, "batch_delete")

        if batch_delete:
            debug("Using batch deletes")

            # Delete the keys in batches
            batch_delete_size = config.getint(uri.site, "batch_delete_size")
            debug("batch_delete_size: %d" % batch_delete_size)
            batch = []
            for k in keys_to_delete:
                batch.append(k)
                if len(batch) == batch_delete_size:
                    info("Deleting batch of %d keys" % len(batch))
                    b.delete_keys(batch, quiet=True)
                    batch = []

            # Delete the final batch
            if len(batch) > 0:
                info("Deleting batch of %d keys" % len(batch))
                b.delete_keys(batch, quiet=True)

        else:
            for key_name in keys_to_delete:
                debug("Deleting %s" % key_name)
                b.delete_key(key_name)


def PartialUpload(up, part, parts, fname, offset, length):
    def upload():
        info("Uploading part %d of %d" % (part, parts))
        f = open(fname, "rb")
        f.seek(offset, os.SEEK_SET)
        up.upload_part_from_file(f, part, size=length)
        info("Finished uploading part %d (%s bytes)" % (part, length))
        f.close()
    return upload

def get_key_for_path(path, infile, outkey):
    if outkey is None or outkey == "":
        raise Exception("invalid key: '%s'" % outkey)

    if not path.startswith("/"):
        raise Exception("path '%s' should be absolute")

    path = path.rstrip("/")
    infile = infile.rstrip("/")

    if not infile.startswith(path):
        raise Exception("file '%s' is not relative to '%s'" % (infile, path))

    if outkey.endswith("/"):
        name = os.path.basename(path)
        outkey = outkey + name

    relpath = os.path.relpath(infile, path)
    if relpath != ".":
        return os.path.join(outkey, relpath)
    else:
        return outkey

def put(args):
    parser = option_parser("put FILE URL")
    parser.add_option("-c", "--chunksize", dest="chunksize", action="store", type="int",
        metavar="X", default=10, help="Set the chunk size for multipart uploads to X MB."
        "A value of 0 disables multipart uploads. The default is 10MB, the min is 5MB "
        "and the max is 1024MB. This parameter only applies for sites that support "
        "multipart uploads (see multipart_uploads configuration parameter). The maximum "
        "number of chunks is 10,000, so if you are uploading a large file, then the "
        "chunksize is automatically increased to enable the upload. Choose smaller values "
        "to reduce the impact of transient failures.")
    parser.add_option("-p", "--parallel", dest="parallel", action="store", type="int",
        metavar="N", default=4, help="Use N threads to upload FILE in parallel. "
            "The default value is 4, which enables parallel uploads with 4 threads. This "
            "parameter is only valid if the site supports mulipart uploads and the "
            "--chunksize parameter is not 0. Otherwise parallel uploads are disabled.")
    parser.add_option("-b", "--create-bucket", dest="create_bucket", action="store_true",
        default=False, help="Create the destination bucket if it does not already exist")
    parser.add_option("-f", "--force", dest="force", action="store_true",
        default=False, help="Overwrite key if it already exists")
    parser.add_option("-r", "--recursive", dest="recursive", action="store_true",
        default=False, help="Treat FILE as a directory")

    options, args = parser.parse_args(args)

    if options.chunksize!=0 and (options.chunksize < 5 or options.chunksize > 1024):
        parser.error("Invalid chunksize")

    if options.parallel <= 0:
        parser.error("Invalid value for --parallel")

    if len(args) != 2:
        parser.error("Specify FILE and URL")

    path = fix_file(args[0])
    url = args[1]

    if not os.path.exists(path):
        raise Exception("No such file or directory: %s" % path)

    # We need the path to be absolute to make it easier to compute relative
    # paths in the recursive mode of operation
    path = os.path.abspath(path)

    # Get a list of all the files to transfer
    if options.recursive:
        if os.path.isfile(path):
            infiles = [path]
            # We can turn off the recursive option if it is a single file
            options.recursive = False
        elif os.path.isdir(path):
            def subtree(dirname):
                result = []
                for name in os.listdir(dirname):
                    path = os.path.join(dirname, name)
                    if os.path.isfile(path):
                        result.append(path)
                    elif os.path.isdir(path):
                        result.extend(subtree(path))
                return result
            infiles = subtree(path)
    else:
        if os.path.isdir(path):
            raise Exception("%s is a directory. Try --recursive." % path)
        infiles = [path]

    # Validate URL
    uri = parse_uri(url)
    if uri.bucket is None:
        raise Exception("URL for put must have a bucket: %s" % url)
    if uri.key is None:
        uri.key = os.path.basename(path)

    config = get_config(options)
    max_object_size = config.getint(uri.site, "max_object_size")

    # Does the site support multipart uploads?
    multipart_uploads = config.getboolean(uri.site, "multipart_uploads")

    # Warn the user
    if options.parallel > 0:
        if not multipart_uploads:
            warn("Multipart uploads disabled, ignoring --parallel ")
        elif options.chunksize == 0:
            warn("--chunksize set to 0, ignoring --parallel")

    conn = get_connection(config, uri)

    # Create the bucket if the user requested it and it does not exist
    if options.create_bucket:
        if conn.lookup(uri.bucket):
            info("Bucket %s exists" % uri.bucket)
        else:
            info("Creating bucket %s" % uri.bucket)
            conn.create_bucket(uri.bucket, location=conn.location)

    b = Bucket(connection=conn, name=uri.bucket)

    info("Uploading %d files" % len(infiles))

    start = time.time()
    totalsize = 0
    for infile in infiles:
        keyname = get_key_for_path(path, infile, uri.key)

        info("Uploading %s to %s/%s" % (infile, uri.bucket, keyname))

        # Make sure file is not too large for the service
        size = os.stat(infile).st_size
        if size > (max_object_size*GB):
            raise Exception("File %s exceeds object size limit"
            " (%sGB) of service" % (infile, max_object_size))
        totalsize += size

        k = Key(bucket=b, name=keyname)

        # Make sure the key does not exist
        if not options.force and k.exists():
            raise Exception("Key exists: '%s'. Try --force." % k.name)

        if (not multipart_uploads) or (options.chunksize==0):
            # no multipart, or chunks disabled, just do it the simple way
            k.set_contents_from_filename(infile)
        else:
            # Multipart supported, chunking requested

            # The target chunk size is user-defined, but we may need
            # to go larger if the file is big because the maximum number
            # of chunks is 10,000. So the actual size of a chunk
            # will range from 5MB to ~525MB if the maximum object size
            # is 5 TB.
            part_size = max(options.chunksize*MB, size/9999)
            num_parts = int(math.ceil(size / float(part_size)))

            if num_parts <= 1:
                # Serial
                k.set_contents_from_filename(infile)
            else:
                # Parallel

                # Request upload
                info("Creating multipart upload")
                upload = b.initiate_multipart_upload(k.name)
                try:
                    # Create all uploads
                    uploads = []
                    for i in range(0, num_parts):
                        length = min(size-(i*part_size), part_size)
                        up = PartialUpload(upload, i+1, num_parts, infile, i*part_size, length)
                        uploads.append(up)

                    if options.parallel <= 1:
                        # Serial
                        for up in uploads:
                            up()
                    else:
                        # Parallel

                        # Queue up requests
                        queue = Queue.Queue()
                        for up in uploads:
                            queue.put(up)

                        # No sense forking more threads than there are chunks
                        nthreads = min(options.parallel, num_parts)

                        # Fork threads
                        threads = []
                        for i in range(0, nthreads):
                            t = WorkThread(queue)
                            threads.append(t)
                            t.start()

                        # Wait for the threads
                        for t in threads:
                            t.join()
                            # If any of the threads encountered
                            # an error, then we fail here
                            if t.exception is not None:
                                raise t.exception

                    info("Completing upload")
                    upload.complete_upload()
                except Exception as e:
                    # If there is an error, then we need to try and abort
                    # the multipart upload so that it doesn't hang around
                    # forever on the server.
                    try:
                        info("Aborting multipart upload")
                        upload.cancel_upload()
                    except Exception as f:
                        sys.stderr.write("ERROR: Unable to abort multipart"
                            " upload (use lsup/rmup): %s\n" % f)
                    raise e

    end = time.time()
    totalsize = totalsize / 1024.0
    elapsed = end - start
    if elapsed > 0:
        rate = totalsize / elapsed
    else:
        rate = 0.0

    info("Uploaded %d files of %0.1f KB in %0.6f seconds: %0.2f KB/s" %
            (len(infiles), totalsize, elapsed, rate))

def lsup(args):
    parser = option_parser("lsup URL")
    options, args = parser.parse_args(args)

    if len(args) == 0:
        parser.error("Specify URL")

    uri = parse_uri(args[0])

    if uri.bucket is None:
        raise Exception("URL must contain a bucket: %s" % args[0])
    if uri.key is not None:
        raise Exception("URL cannot contain a key: %s" % args[0])

    config = get_config(options)
    conn = get_connection(config, uri)

    b = conn.get_bucket(uri.bucket)

    for up in b.list_multipart_uploads():
        uri.key = up.key_name
        sys.stdout.write("%s %s\n" % (uri, up.id))

def rmup(args):
    parser = option_parser("rmup URL [UPLOAD]")
    parser.add_option("-a", "--all", dest="all", action="store_true",
        default=False, help="Cancel all uploads for the specified bucket")
    options, args = parser.parse_args(args)

    if options.all:
        if len(args) < 1:
            parser.error("Specify bucket URL")
    else:
        if len(args) != 2:
            parser.error("Specify bucket URL and UPLOAD")
        upload = args[1]

    uri = parse_uri(args[0])

    if uri.bucket is None:
        raise Exception("URL must contain a bucket: %s" % args[0])
    if uri.key is not None:
        raise Exception("URL cannot contain a key: %s" % args[0])

    config = get_config(options)
    conn = get_connection(config, uri)

    # There is no easy way to do this with boto
    b = Bucket(connection=conn, name=uri.bucket)
    for up in b.list_multipart_uploads():
        if options.all or up.id == upload:
            info("Removing upload %s" % up.id)
            up.cancel_upload()

def PartialDownload(bucketname, keyname, fname, part, parts, start, end):
    def download():
        info("Downloading part %d of %d" % (part, parts))
        f = open(fname, "r+b")

        attempt = 0
        done = False
        saved_ex = None
        while attempt < 3 and done == False:
            attempt += 1
            try:
                f.seek(start, os.SEEK_SET)
                # Need to use a different key object to each thread because
                # the Key object in boto is not thread-safe
                key = Key(bucket=bucketname, name=keyname)
                key.get_file(f, headers={"Range": "bytes=%d-%d" % (start, end)})
                done = True
            except Exception as e:
                saved_ex = e
                debug("Attempt %d failed for part %d" %(attempt, part))
        if done == False:
            raise saved_ex

        info("Part %d finished" % (part))
        f.close()

    return download

def get_path_for_key(bucket, searchkey, key, output):
    # We have to strip any trailing / off the keys so that they can match
    # Also, if a key is None, then convert it to an empty string
    key = "" if key is None else key.rstrip("/")
    searchkey = "" if searchkey is None else searchkey.rstrip("/")

    # If output ends with a /, then we need to add a name onto it
    if output.endswith("/"):
        name = bucket if searchkey == "" else os.path.basename(searchkey)
        output = os.path.join(output, name)

    if searchkey == key:
        # If they are the same, then return the new output path
        return output
    else:
        # Otherwise we need to compute the relative path and add it
        relpath = os.path.relpath(key, searchkey)
        return os.path.join(output, relpath)

def get(args):
    parser = option_parser("get URL [FILE]")
    parser.add_option("-c", "--chunksize", dest="chunksize", action="store", type="int",
        metavar="X", default=10, help="Set the chunk size for parallel downloads to X "
        "megabytes. A value of 0 will avoid chunked reads. This option only applies for "
        "sites that support ranged downloads (see ranged_downloads configuration "
        "parameter). The default chunk size is 10MB, the min is 1MB and the max is "
        "1024MB. Choose smaller values to reduce the impact of transient failures.")
    parser.add_option("-p", "--parallel", dest="parallel", action="store", type="int",
        metavar="N", default=4, help="Use N threads to upload FILE in parallel. The "
            "default value is 4, which enables parallel downloads with 4 threads. "
            "This parameter is only valid if the site supports ranged downloads "
            "and the --chunksize parameter is not 0. Otherwise parallel downloads are "
            "disabled.")
    parser.add_option("-r", "--recursive", dest="recursive", action="store_true",
        help="Get all keys that start with URL")
    options, args = parser.parse_args(args)

    if options.chunksize < 0 or options.chunksize > 1024:
        parser.error("Invalid chunksize")

    if options.parallel <= 0:
        parser.error("Invalid value for --parallel")

    if len(args) == 0:
        parser.error("Specify URL")

    uri = parse_uri(args[0])

    if uri.bucket is None:
        raise Exception("URL must contain a bucket: %s" % args[0])
    if uri.key is None and not options.recursive:
        raise Exception("URL must contain a key or use --recursive")

    if len(args) > 1:
        output = fix_file(args[1])
    elif uri.key is None:
        output = "./"
    else:
        output = os.path.basename(uri.key.rstrip("/"))

    info("Downloading %s" % uri)

    # Does the site support ranged downloads properly?
    config = get_config(options)
    ranged_downloads = config.getboolean(uri.site, "ranged_downloads")

    # Warn the user
    if options.parallel > 1:
        if not ranged_downloads:
            warn("ranged downloads not supported, ignoring --parallel")
        elif options.chunksize == 0:
            warn("--chunksize set to 0, ignoring --parallel")

    conn = get_connection(config, uri)
    b = Bucket(connection=conn, name=uri.bucket)

    if options.recursive:
        # Get all the keys we need to download

        def keyfilter(k):
            if uri.key is None:
                # We want all the keys in the bucket
                return True

            if uri.key.endswith("/"):
                # The user specified a "folder", so we should only match keys
                # in that "folder"
                return k.name.startswith(uri.key)

            if k.name == uri.key:
                # Match bare keys in case they specify recursive, but there
                # is a key that matches the specified path. Note that this
                # could cause a problem in the case where they have a key
                # called 'foo' and a "folder" called 'foo' in the same
                # bucket. In a file system that can't happen, but it can
                # happen in S3.
                return True

            if k.name.startswith(uri.key+"/"):
                # All other keys in the "folder"
                return True

            return False

        keys = [x for x in b.list(uri.key) if keyfilter(x)]
    else:
        # Just get the one key we need to download
        key = b.get_key(uri.key)
        if key is None:
            raise Exception("No such key. If %s is a folder, try --recursive." % uri.key)
        keys = [key]

    info("Downloading %d keys" % len(keys))

    start = time.time()
    totalsize = 0
    for key in keys:
        outfile = get_path_for_key(b.name, uri.key, key.name, output)

        info("Downloading %s/%s to %s" % (uri.bucket, key.name, outfile))

        outfile = os.path.abspath(outfile)

        # This means that the key is a "folder", so we just need to create
        # a directory for it
        if key.name.endswith("/") and key.size == 0:
            if not os.path.isdir(outfile):
                os.makedirs(outfile)
            continue

        if os.path.isdir(outfile):
            raise Exception("%s is a directory" % outfile)

        outdir = os.path.dirname(outfile)
        if not os.path.isdir(outdir):
            os.makedirs(outdir)

        # We need this for the performance report
        totalsize += key.size

        if (not ranged_downloads) or (options.chunksize == 0):
            # Ranged downloads not supported, or chunking disabled
            key.get_contents_to_filename(outfile)
        else:
            # Ranged downloads and chunking requested

            # Compute chunks
            part_size = options.chunksize*MB
            num_parts = int(math.ceil(key.size / float(part_size)))

            if num_parts <= 1:
                # No point if there is only one chunk
                key.get_contents_to_filename(outfile)
            else:
                # Create the file and set it to the appropriate size.
                f = open(outfile, "w+b")
                f.seek(key.size-1)
                f.write('\0')
                f.close()

                # Create all the downloads
                downloads = []
                for i in range(0, num_parts):
                    dstart = i*part_size
                    dend = min(key.size, dstart+part_size-1)
                    down = PartialDownload(b, key.name, outfile, i+1, num_parts, dstart, dend)
                    downloads.append(down)

                if options.parallel <= 1:
                    # Serial
                    for down in downloads:
                        down()
                else:
                    # Parallel

                    # No sense forking more threads than there are chunks
                    nthreads = min(options.parallel, num_parts)

                    info("Starting parallel download with %d threads" % nthreads)

                    # Queue up requests
                    queue = Queue.Queue()
                    for down in downloads:
                        queue.put(down)

                    # Fork threads
                    threads = []
                    for i in range(0, nthreads):
                        t = WorkThread(queue)
                        threads.append(t)
                        t.start()

                    # Wait for the threads
                    for t in threads:
                        t.join()
                        # If any of the threads encountered
                        # an error, then we fail here
                        if t.exception is not None:
                            raise t.exception

    end = time.time()
    totalsize = totalsize / 1024.0
    elapsed = end - start
    if elapsed > 0:
        rate = totalsize / elapsed
    else:
        rate = 0.0

    info("Downloaded %d keys of %0.1f KB in %0.6f seconds: %0.2f KB/s" %
            (len(keys), totalsize, elapsed, rate))

def main():
    if len(sys.argv) < 2:
        help()
        exit(1)

    command = sys.argv[1].lower()
    args = sys.argv[2:]

    if command in COMMANDS:
        fn = globals()[command]
        try:
            fn(args)
        except Exception as e:
            # Just raise the exception if the user wants more info
            if VERBOSE or DEBUG: raise

            error = str(e)

            # This is for boto S3ResponseError
            if hasattr(e, "error_message"):
                error = e.error_message or error

            sys.stderr.write("ERROR: %s\n" % error)
            exit(1)
    else:
        sys.stderr.write("ERROR: Unknown command: %s\n" % command)
        help()
        exit(1)
