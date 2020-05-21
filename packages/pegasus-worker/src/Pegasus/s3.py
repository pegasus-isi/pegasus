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

import fnmatch
import math
import os
import re
import stat
import sys
import threading
import time
import traceback
from optparse import OptionParser

from six.moves import queue as Queue
from six.moves.configparser import ConfigParser
from six.moves.urllib.parse import urlsplit

try:
    import boto
    import boto.exception
    import boto.s3.connection
    from boto.s3.bucket import Bucket
    from boto.s3.key import Key

    # boto3
    import boto3
    import botocore
except ImportError as e:
    sys.stderr.write("ERROR: Unable to load boto library: %s\n" % e)
    exit(1)

# do not use http proxies for S3
if "http_proxy" in os.environ:
    del os.environ["http_proxy"]

# Don't let apple hijack our cacerts
os.environ["OPENSSL_X509_TEA_DISABLE"] = "1"

# The multipart upload feature we require was introduced in 2.2.2
boto_version = tuple(int(x) for x in boto.__version__.split("."))
if boto_version < (2, 2, 2):
    sys.stderr.write("Requires boto 2.2.2 or later, not %s\n" % boto.__version__)
    exit(1)

# set boto config options
try:
    boto.config.add_section("Boto")
except Exception:
    pass
boto.config.set("Boto", "http_socket_timeout", "60")

COMMANDS = {
    "ls": "List the contents of a bucket",
    "mkdir": "Create a bucket in S3",
    "rm": "Delete a key from S3",
    "put": "Upload a key to S3 from a file",
    "get": "Download a key from S3 to a file",
    "cp": "Copy keys remotely",
    "help": "Print this message",
}

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB

def human_size(size):
    if size >= TB:
        return "{0:6.1f}TB".format(size / float(TB))
    elif size >= GB:
        return "{0:6.1f}GB".format(size / float(GB))
    elif size >= MB:
        return "{0:6.1f}MB".format(size / float(MB))
    elif size >= KB:
        return "{0:6.1f}KB".format(size / float(KB))
    else:
        return "{0:6.0f}B".format(size)

# see https://docs.aws.amazon.com/general/latest/gr/s3.html
LOCATIONS = {
    "s3.amazonaws.com": "",
    "s3.us-east-1.amazonaws.com": "us-east-1",
    "s3.us-east-2.amazonaws.com": "us-east-2",
    "s3-us-west-1.amazonaws.com": "us-west-1",
    "s3-us-west-2.amazonaws.com": "us-west-2",
    "s3-ca-central-1.amazonaws.com": "ca-central-1",
    "s3-eu-west-1.amazonaws.com": "EU",
    "s3-ap-southeast-1.amazonaws.com": "ap-southeast-1",
    "s3-ap-southeast-2.amazonaws.com": "ap-southeast-2",
    "s3-ap-northeast-1.amazonaws.com": "ap-northeast-1",
    "s3-ap-northeast-2.amazonaws.com": "ap-northeast-2",
    "s3-ap-northeast-3.amazonaws.com": "ap-northeast-3",
    "s3-ap-south-1.amazonaws.com": "ap-south-1",
    "s3-sa-east-1.amazonaws.com": "sa-east-1",
}

DEFAULT_CONFIG = {
    "max_object_size": str(5),
    "multipart_uploads": str(False),
    "ranged_downloads": str(False),
    "batch_delete": str(True),
    "batch_delete_size": str(1000),
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
        url = url.replace("file:", "")
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
    parser.add_option(
        "-d",
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="Turn on debugging",
    )
    parser.add_option(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        default=False,
        help="Show progress messages",
    )
    parser.add_option(
        "-C",
        "--conf",
        dest="config",
        metavar="FILE",
        default=None,
        help="Path to configuration file",
    )

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
        "is_secure": result.scheme == "https",
        "host": result.hostname,
        "port": result.port,
        "path": result.path,
    }

    location = LOCATIONS.get(result.hostname, "")

    return kwargs, location


def get_connection(config, uri):
    if not config.has_section(uri.site):
        raise Exception("Config file has no section for site '%s'" % uri.site)

    if not config.has_section(uri.ident):
        raise Exception("Config file has no section for identity '%s'" % uri.ident)

    endpoint = config.get(uri.site, "endpoint")
    kwargs, location = parse_endpoint(endpoint)

    kwargs["aws_access_key_id"] = config.get(uri.ident, "access_key")
    kwargs["aws_secret_access_key"] = config.get(uri.ident, "secret_key")
    kwargs["calling_format"] = boto.s3.connection.OrdinaryCallingFormat()

    # If the URI is s3s, then override the config
    if uri.secure:
        kwargs["validate_certs"] = True
        kwargs["is_secure"] = True

    conn = boto.s3.connection.S3Connection(**kwargs)
    conn.location = location

    return conn


def get_s3_client(config, uri):
    if not config.has_section(uri.site):
        raise Exception("Config file has no section for site '%s'" % uri.site)

    if not config.has_section(uri.ident):
        raise Exception("Config file has no section for identity '%s'" % uri.ident)

    endpoint = config.get(uri.site, "endpoint")
    aws_access_key_id = config.get(uri.ident, "access_key")
    aws_secret_access_key = config.get(uri.ident, "secret_key")

    # what about s3s????

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


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
        http = uri.replace("s3s://", "http://")
    else:
        http = uri.replace("s3://", "http://")
    result = urlsplit(http)

    # The user should not be specifying a query part unless
    # they are trying to use the ? wildcard. If they do use
    # the ? wildcard, then urlsplit thinks it is the query
    # separator. In that case, we put the path and query
    # back together.
    if "?" in uri:
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
        comp = path.split("/", 1)
        bucket = comp[0]
        if len(comp) == 1:
            key = None
        elif comp[1] == "":
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

    parser.add_option(
        "-l",
        "--long",
        dest="long_format",
        action="store_true",
        default=False,
        help="Use long listing format",
    )
    parser.add_option(
        "-H",
        "--human-sized",
        dest="human_sized",
        action="store_true",
        default=False,
        help="Use human readable sizes",
    )

    options, args = parser.parse_args(args)

    if len(args) == 0 or len(args) > 1:
        parser.error("Specify a single URL")

    config = get_config(options)
    uri = parse_uri(args.pop())
    
    s3 = get_s3_client(config, uri)
    
    if uri.bucket:
        # list keys in bucket
        try:
            keys = s3.list_objects_v2(
                        Bucket=uri.bucket,
                        Prefix=uri.key if uri.key else "",
                        FetchOwner=True
                    )
        except s3.exceptions.NoSuchBucket:
            print("Invalid bucket: {}".format(uri.bucket))
            sys.exit(1)
        except botocore.exceptions.ClientError as e:
            # endpoint may also raise this for invalid bucket name
            if e.response["Error"]["Code"] == "InvalidBucketName":
                print("Invalid bucket: {}".format(uri.bucket))
                sys.exit(1)
            else:
                raise e

        if keys.get("Contents"):
            for content in keys["Contents"]:
                key = content["Key"]

                if options.long_format:
                    size = human_size(content["Size"]) if options.human_sized else "{0:13d}".format(content["Size"])
                    last_modified = content["LastModified"]
                    owner = content["Owner"]["DisplayName"]
                    storage_class = content["StorageClass"]
                    print("\t{owner:15s} {size} {modified} {storage_class:24s} {name}".format(
                            owner=owner,
                            size=size,
                            modified=last_modified,
                            storage_class=storage_class,
                            name=key
                        ))
                else:
                    print("\t{}".format(key))
    else:
        # list buckets
        buckets = s3.list_buckets()
        for b in buckets["Buckets"]:
            print("\t{}".format(b["Name"]))

def cp(args):
    parser = option_parser("cp SRC[...] DEST")

    parser.add_option(
        "-c",
        "--create-dest",
        dest="create",
        action="store_true",
        default=False,
        help="Create destination bucket if it does not exist",
    )
    parser.add_option(
        "-r",
        "--recursive",
        dest="recurse",
        action="store_true",
        default=False,
        help="If SRC is a bucket, copy all its keys to DEST",
    )
    parser.add_option(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="If DEST key exists, then overwrite it",
    )

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
            raise Exception("Source URL does not contain a key " "(see -r): %s" % src)

        # Each source must have the same identity as the destination.
        # Copying from one account to another, or one region to another,
        # is not allowed.
        if src.ident != dest.ident:
            raise Exception(
                "Identities for source and destination "
                "do not match: %s -> %s" % (src, dest)
            )

    conn = get_connection(config, dest)
    destbucket = conn.lookup(dest.bucket)
    if destbucket is None:
        if options.create:
            info("Creating destination bucket %s" % dest.bucket)
            destbucket = conn.create_bucket(dest.bucket, location=conn.location)
        else:
            raise Exception(
                "Destination bucket %s does not exist " "(see -c)" % dest.bucket
            )

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
                    raise Exception(
                        "Destination key %s exists " "(see -f)" % destkeyname
                    )

            info(
                "Copying %s/%s to %s/%s"
                % (src.bucket, srckey.name, dest.bucket, destkeyname)
            )

            start_time = time.time()

            # If the source is larger than 5GB we need to use multipart uploads
            if multipart_uploads and srckey.size > 5 * GB:
                info("Object is larger than 5GB: Using multipart uploads")
                i = 1
                start = 0
                parts = math.ceil(srckey.size / (1.0 * GB))
                mp = destbucket.initiate_multipart_upload(destkeyname)
                while start < srckey.size:
                    end = min(start + GB - 1, srckey.size - 1)
                    info("Copying part %d of %d (%d-%d)" % (i, parts, start, end))
                    mp.copy_part_from_key(src.bucket, srckey.name, i, start, end)
                    i += 1
                    start = end + 1
                mp.complete_upload()
            else:
                destbucket.copy_key(
                    new_key_name=destkeyname,
                    src_bucket_name=src.bucket,
                    src_key_name=srckey.name,
                )

            end_time = time.time()

            size = srckey.size / 1024.0
            elapsed = end_time - start_time
            rate = size / elapsed

            info("Copied %0.1f KB in %0.6f seconds: %0.2f KB/s" % (size, elapsed, rate))

def mkdir(args):
    parser = option_parser("mkdir URL...")

    parser.add_option(
        "-r",
        "--region",
        dest="region",
        action="store_true",
        default=None,
        help="Create the destination bucket if it does not already exist",
    )

    options, args = parser.parse_args(args)

    if len(args) == 0:
        parser.error("Specify URL")

    if uri.bucket is None:
        print("URL for mkdir must contain a bucket: %s" % arg)
        sys.exit(1)
    if uri.key is not None:
        print("URL for mkdir cannot contain a key: %s" % arg)
        sys.exit(1)

    s3 = get_s3_client(config, uri)

    create_bucket_config = {}
    if parser.region:
        create_bucket_config = {
            "LocationConstrain": options.region
        }

    is_duplicate_bucket = False
    try:
        s3.create_bucket(Bucket=uri.bucket, CreateBucketConfiguration=create_bucket_config)
    except s3.exceptions.BucketAlreadyExists:
        print("Bucket: {} already taken".format(uri.bucket))
        sys.exit(1)

    except s3.exceptions.BucketAlreadyOwnedByYou:
        is_duplicate_bucket = True
    
    if is_duplicate_bucket:
        print("Bucket: {} is already owned by you".format(uri.bucket))
    else:
        print("Bucket: {} has been created".format(uri.bucket))

def rm(args):
    parser = option_parser("rm URL...")
    parser.add_option(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="Ignore nonexistent keys",
    )
    parser.add_option(
        "-F",
        "--file",
        dest="file",
        action="store",
        default=None,
        help="File containing a list of URLs to delete",
    )
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
        try:
            s3 = get_s3_client(config, uri) 

            # Get a final list of all the keys, resolving wildcards as necessary
            bucket_contents = None
            keys_to_delete = set()
            for key in keys:
                key_name = key.key

                if has_wildcards(key_name):

                    # If we haven't yet queried the bucket, then do so now
                    # so that we can match the wildcards
                    if bucket_contents is None:
                        #bucket_contents = b.list()
                        keys = s3.list_objects_v2(Bucket=uri.bucket)
                        try:
                            bucket_contents = [obj["Key"] for obj in keys["Contents"]]
                        except KeyError:
                            print("Unable to fetch objects list from bucket: {}".format(uri.bucket))
                            sys.exit(1)

                    # Collect all the keys that match
                    for k in bucket_contents:
                        if fnmatch.fnmatch(k.name, key_name):
                            keys_to_delete.add(k.name)

                else:
                    keys_to_delete.add(key_name)

            info("Deleting %d keys" % len(keys_to_delete))

            batch_delete = config.getboolean(uri.site, "batch_delete")

            # TODO: what about versioned buckets?
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
                        #b.delete_keys(batch, quiet=True)

                        resp = s3.delete_objects(
                                    Bucket=uri.bucket,
                                    Delete={
                                        "Objects": [{"Key": item} for item in batch]
                                    }
                                )

                        if not len(resp["Deleted"]) == len(batch):
                            print("Incomplete batch delete, some keys were not successfully deleted.")
                            sys.exit(1)

                        batch = []

                # Delete the final batch
                if len(batch) > 0:
                    info("Deleting batch of %d keys" % len(batch))
                    resp = s3.delete_objects(
                                Bucket=uri.bucket,
                                Delete={
                                    "Objects": [{"Key": item} for item in batch]
                                }
                            )

                    if not len(resp["Deleted"]) == len(batch):
                        print("Incomplete batch delete, some keys were not successfully deleted.")
                        sys.exit(1)

            else:
                for key_name in keys_to_delete:
                    debug("Deleting %s" % key_name)
                    s3.delete_object(Bucket=uri.bucket, Key=key_name)


        except s3.exceptions.NoSuchBucket:
            print("Invalid bucket: {}".format(uri.bucket))
            sys.exit(1)
        except botocore.exceptions.ClientError as e:
            # endpoint may also raise this for invalid bucket name
            if e.response["Error"]["Code"] == "InvalidBucketName":
                print("Invalid bucket: {}".format(uri.bucket))
                sys.exit(1)
            else:
                raise e


def rm_old(args):
    parser = option_parser("rm URL...")
    parser.add_option(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="Ignore nonexistent keys",
    )
    parser.add_option(
        "-F",
        "--file",
        dest="file",
        action="store",
        default=None,
        help="File containing a list of URLs to delete",
    )
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

    parser.add_option(
        "-b",
        "--create-bucket",
        dest="create_bucket",
        action="store_true",
        default=False,
        help="Create the destination bucket if it does not already exist",
    )
    parser.add_option(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="Overwrite key if it already exists",
    )
    parser.add_option(
        "-r",
        "--recursive",
        dest="recursive",
        action="store_true",
        default=False,
        help="Treat FILE as a directory",
    )

    options, args = parser.parse_args(args)

    path = fix_file(args[0])
    url = args[1]

    if not os.path.exists(path):
        print("No such file or directory: {}".format(path))
        sys.exit(1)

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

            subtree(path)
    else:
        if os.path.isdir(path):
            raise Exception("%s is a directory. Try --recursive." % path)
        infiles = [path]

    log.info("Attempting to upload {} files".format(len(infiles)))

    # Validate URL
    uri = parse_uri(url)
    if uri.bucket is None:
        print("URL for put must have a bucket: %s" % url)
        sys.exit(1)
    if uri.key is None:
        uri.key = os.path.basename(path)

    config = get_config(options)
    max_object_size = config.getint(uri.site, "max_object_size")

    # get s3 client with associated endpoint
    s3 = get_s3_client(config, uri)

    # Create the bucket if the user requested it and it does not exist
    if options.create_bucket:
        try:
            resp = s3.create_bucket(Bucket=uri.bucket)
        except s3.exceptions.BucketAlreadyExists:
            print("Bucket: {} already taken".format(uri.bucket))
            sys.exit(1)
        except s3.exceptions.BucketAlreadyOwnedByYou:
            # AWS S3 endpoint will throw this, but other endpoints may not
            pass
    
    if not options.force:
        # check if all keys do not yet exist
        # accepted method of checking for existence of a key 
        key_already_exists = False
        pre_existing_key = ""
        for f in infiles:
            try:
                s3.head_object(Bucket=uri.bucket, Key=f)
                
                key_already_exists = True
                pre_existing_key = f
                break
            except s3.exceptions.ClientError as e:
                error_code = e.response["ResponseMetadata"]["HTTPStatusCode"]

                if error_code == 403:
                    print("Access to bucket: {bucket}, key: {key} forbidden".format(bucket=uri.bucket, key=f))
                elif error_code == 404:
                    # 
                    pass
                else:
                    print("Unknown client error")
                    sys.exit(1)

        if key_already_exists:
            print("Key: {} already exists. Trye --force to overwrite".format(pre_existing_key))
            sys.exit(1)

    for f in infiles:
        try:
            key = f if uri.key is None else uri.key
            s3.upload_file(f, uri.bucket, key)
            print("Uploaded file: {file} to bucket: {bucket} as key: {key}".format(
                file=f,
                bucket=uri.bucket,
                key=key
            ))
        except boto3.exceptions.S3UploadFailedError:
            print("Failed to upload file: {file} to bucket: {bucket} as key: {key}".format(
                file=f,
                bucket=uri.bucket,
                key=key
            ))
            sys.exit(1) 
    
    print("Successfully uploaded {} files".format(len(infiles)))

def PartialDownload(bucketname, keyname, fname, part, parts, start, end):
    def download():
        info("Downloading part %d of %d" % (part, parts))
        f = open(fname, "r+b")

        attempt = 0
        done = False
        saved_ex = None
        while attempt < 3 and done is False:
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
                debug("Attempt %d failed for part %d" % (attempt, part))
        if done is False:
            raise saved_ex

        info("Part %d finished" % (part))
        f.close()

    return download

def get(args):
    parser = option_parser("get URL [FILE]")
    options, args = parser.parse_args(args)

    if len(args) == 0:
        parser.error("Specify URL")

    uri = parse_uri(args[0])

    if uri.bucket is None:
        raise Exception("URL must contain a bucket: %s" % args[0])
    if uri.key is None :
        raise Exception("URL must contain a key")

    if len(args) > 1:
        output = fix_file(args[1])
    else:
        output = os.path.basename(uri.key.rstrip("/"))

    info("Downloading %s" % uri)

    config = get_config(options)
    s3 = get_s3_client(config, uri)

    try:            
        s3.download_file(
            Bucket=uri.bucket,
            Key=uri.key,
            Filename=output
        )
    except s3.exceptions.NoSuchBucket:
        print("Invalid bucket: {}".format(uri.bucket))
        sys.exit(1)
    except botocore.exceptions.ClientError as e:
        # endpoint may also raise this for invalid bucket name
        if e.response["Error"]["Code"] == "InvalidBucketName":
            print("Invalid bucket: {}".format(uri.bucket))
            sys.exit(1)
        else:
            raise e
    
    info("Download: {} complete".format(uri))

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
            if VERBOSE or DEBUG:
                raise

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
