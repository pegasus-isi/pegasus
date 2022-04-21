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

import logging as log
import os
import re
import stat
import sys
from argparse import ArgumentParser

from six.moves.configparser import ConfigParser
from six.moves.urllib.parse import urlsplit

try:
    import boto3
    import botocore
except ImportError as e:
    sys.stderr.write("ERROR: Unable to load boto3 library: %s\n" % e)
    exit(1)

# do not use http proxies for S3
if "http_proxy" in os.environ:
    del os.environ["http_proxy"]

# Don't let apple hijack our cacerts
os.environ["OPENSSL_X509_TEA_DISABLE"] = "1"

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


def configure_logging(verbose, debug):

    if verbose or debug:
        if verbose:
            level = log.INFO

        if debug:
            level = log.DEBUG

        log.basicConfig(
            level=level,
            format="%(asctime)s:%(levelname)s:%(name)s(%(lineno)d): %(message)s",
        )


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
    "batch_delete": str(True),
    "batch_delete_size": str(1000),
}


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


DEFAULT_CREDENTIAL_PATH = "~/.pegasus/credentials.conf"
OLD_DEFAULT_CREDENTIAL_PATH = "~/.pegasus/s3cfg"


def get_config(options):
    PEGASUS_CREDENTIALS = os.getenv("PEGASUS_CREDENTIALS", None)
    S3CFG = os.getenv("S3CFG", None)
    if options.config:
        # Command-line overrides everything
        cfg = options.config
    elif PEGASUS_CREDENTIALS is not None:
        # Environment variable overrides defaults
        cfg = PEGASUS_CREDENTIALS
    elif S3CFG is not None:
        # Environment variable overrides defaults
        cfg = S3CFG
    else:
        # New default
        new_default = os.path.expanduser(DEFAULT_CREDENTIAL_PATH)
        if os.path.isfile(new_default):
            cfg = new_default
        else:
            # If the new default doesn't exist, try the old default
            cfg = os.path.expanduser(OLD_DEFAULT_CREDENTIAL_PATH)

    if not os.path.isfile(cfg):
        raise Exception("Config file not found")

    log.info("Found config file: %s" % cfg)

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


def is_bucket_available(s3_client, bucket):
    is_available = False
    try:
        s3_client.head_bucket(Bucket=bucket)
        is_available = True
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]

        # Not Found
        if code == "404":
            is_available = True

        # Anything other than 403 Forbidden, won't handle
        elif code != "403":
            raise e

    return is_available


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
    config = get_config(args)
    uri = parse_uri(args.url)

    s3 = get_s3_client(config, uri)

    if uri.bucket:
        # list keys in bucket
        try:
            keys = s3.list_objects_v2(
                Bucket=uri.bucket, Prefix=uri.key if uri.key else "", FetchOwner=True
            )
        except s3.exceptions.NoSuchBucket:
            raise Exception("Invalid bucket: {}".format(uri.bucket))
        except botocore.exceptions.ClientError as e:
            # endpoint may also raise this for invalid bucket name
            if e.response["Error"]["Code"] == "InvalidBucketName":
                raise Exception("Invalid bucket: {}".format(uri.bucket))
            else:
                raise e

        if keys.get("Contents"):
            for content in keys["Contents"]:
                key = content["Key"]

                if args.long_format:
                    size = (
                        human_size(content["Size"])
                        if args.human_sized
                        else "{0:13d}".format(content["Size"])
                    )
                    last_modified = content["LastModified"]
                    owner = content["Owner"]["DisplayName"]
                    storage_class = content["StorageClass"]
                    print(
                        "\t{owner:15s} {size} {modified} {storage_class:24s} {name}".format(
                            owner=owner,
                            size=size,
                            modified=last_modified,
                            storage_class=storage_class,
                            name=key,
                        )
                    )
                else:
                    print("\t{}".format(key))
    else:
        # list buckets
        buckets = s3.list_buckets()
        for b in buckets["Buckets"]:
            print("\t{}".format(b["Name"]))


def cp(args):
    config = get_config(args)

    srcs = [parse_uri(uri) for uri in args.srcs]
    dest = parse_uri(args.dest)

    # If there is more than one source, then the destination must be
    # a bucket and not a bucket+key.
    if len(srcs) > 1 and dest.key is not None:
        raise Exception("Destination must be a bucket if there are multiple sources")

    # Validate all the URI pairs
    for src in srcs:
        # The source URI must have a key
        if src.key is None:
            raise Exception("Source URL does not contain a key: %s" % src)

        # Each source must have the same identity as the destination.
        # Copying from one account to another, or one region to another,
        # is not allowed.
        if src.ident != dest.ident:
            raise Exception(
                "Identities for source and destination "
                "do not match: %s -> %s" % (src, dest)
            )

    # using the first source (from the checks above, it is garuanteed that
    # identities all match)
    s3 = get_s3_client(config, srcs[0])

    # Create the bucket if the user requested it and it does not exist
    if args.create:
        can_create = True
        try:
            s3.head_bucket(Bucket=dest.bucket)

            # no exception, we already own this bucket
            can_create = False
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]

            # 403 forbidden means bucket already taken
            if code == "403":
                raise Exception(
                    "Bucket: {} is already taken. Unable to create bucket.".format(
                        dest.bucket
                    )
                )

            # 404 not found means bucket can be created
            elif code != "404":
                raise e

        if can_create:
            s3.create_bucket(Bucket=dest.bucket)

    # ensure that none of the keys in srcs exist in dest
    if not args.force:
        if dest.key == None:
            for src in srcs:
                try:
                    s3.head_object(Bucket=dest.bucket, Key=src.key)
                    raise Exception(
                        "Key: {key} already exists in destination bucket: {bucket}, (see --force)".format(
                            key=src.key, bucket=dest.bucket
                        )
                    )
                except botocore.exceptions.ClientError as e:
                    error_code = e.response["ResponseMetadata"]["HTTPStatusCode"]

                    if error_code != 404:
                        raise e

        else:
            assert len(srcs) == 1
            src = srcs[0]
            try:
                s3.head_object(Bucket=dest.bucket, Key=dest.key)
                raise Exception(
                    "Key: {key} already exists in destination bucket: {bucket}, (see --force)".format(
                        key=dest.key, bucket=dest.bucket
                    )
                )
            except botocore.exceptions.ClientError as e:
                error_code = e.response["ResponseMetadata"]["HTTPStatusCode"]
                if error_code != 404:
                    raise e

    if dest.key == None:
        for src in srcs:
            s3.copy(
                CopySource={"Bucket": src.bucket, "Key": src.key},
                Bucket=dest.bucket,
                Key=src.key,
            )
    else:
        assert len(srcs) == 1
        src = srcs[0]
        s3.copy(
            CopySource={"Bucket": src.bucket, "Key": src.key},
            Bucket=dest.bucket,
            Key=dest.key,
        )


def mkdir(args):
    uri = parse_uri(args.url)

    if uri.bucket is None:
        raise Exception("URL for mkdir must contain a bucket: %s" % arg)
    if uri.key is not None:
        raise Exception("URL for mkdir cannot contain a key: %s" % arg)

    config = get_config(args)
    s3 = get_s3_client(config, uri)

    can_create = True
    try:
        s3.head_bucket(Bucket=uri.bucket)

        # no exception, we already own this bucket
        can_create = False
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]

        # 403 forbidden means bucket already taken
        if code == "403":
            raise Exception(
                "Bucket: {} is already taken. Unable to create bucket.".format(
                    uri.bucket
                )
            )

        # 404 not found means bucket can be created
        elif code != "404":
            raise e

    if can_create:
        s3.create_bucket(Bucket=uri.bucket)
    else:
        log.warning(
            "Bucket: {} exists and is already owned by user: {}".format(
                uri.bucket, uri.ident
            )
        )


def rm(args):
    if args.url is None and args.file is None:
        print("Specify URL")
        sys.exit(1)

    uris = []
    if args.file:
        for rec in read_command_file(args.file):
            if len(rec) != 1:
                raise Exception("Invalid record: %s" % rec)
            uris.append(rec[0])
    else:
        uris.append(args.url)

    buckets = {}
    for uri in uris:
        uri = parse_uri(uri)
        if uri.bucket is None:
            raise Exception("URL for rm must contain a bucket: %s" % uri)
        if uri.key is None:
            raise Exception("URL for rm must contain a key: %s" % uri)

        bid = "%s/%s" % (uri.ident, uri.bucket)
        buri = S3URI(uri.user, uri.site, uri.bucket, uri.secure)

        if bid not in buckets:
            buckets[bid] = (buri, set())
        buckets[bid][1].add(uri)

    config = get_config(args)

    for bucket in buckets:

        # Connect to the bucket
        log.info("Deleting keys from bucket %s" % bucket)
        uri, keys = buckets[bucket]
        try:
            s3 = get_s3_client(config, uri)

            keys_to_delete = {k.key for k in keys}

            # check that all keys to be deleted exist if force is not set
            if not args.force:
                bucket_contents = {
                    obj["Key"]
                    for obj in s3.list_objects_v2(Bucket=uri.bucket)["Contents"]
                }
                if not keys_to_delete.issubset(bucket_contents):
                    raise Exception(
                        "Some keys to delete do not exist in bucket: {}; use --force to ignore non existing keys".format(
                            uri.bucket
                        )
                    )

            log.info("Deleting %d keys" % len(keys_to_delete))

            batch_delete = config.getboolean(uri.site, "batch_delete")

            if batch_delete:
                log.info("Using batch deletes")

                # Delete the keys in batches
                batch_delete_size = config.getint(uri.site, "batch_delete_size")
                log.info("batch_delete_size: %d" % batch_delete_size)
                batch = []
                for k in keys_to_delete:
                    batch.append(k)
                    if len(batch) == batch_delete_size:
                        log.info("Deleting batch of %d keys" % len(batch))

                        resp = s3.delete_objects(
                            Bucket=uri.bucket,
                            Delete={"Objects": [{"Key": item} for item in batch]},
                        )

                        if not len(resp["Deleted"]) == len(batch):
                            raise Exception(
                                "Incomplete batch delete, some keys were not successfully deleted."
                            )

                        batch = []

                # Delete the final batch
                if len(batch) > 0:
                    log.info("Deleting batch of %d keys" % len(batch))
                    resp = s3.delete_objects(
                        Bucket=uri.bucket,
                        Delete={"Objects": [{"Key": item} for item in batch]},
                    )

                    if not len(resp["Deleted"]) == len(batch):
                        raise Exception(
                            "Incomplete batch delete, some keys were not successfully deleted."
                        )

            else:
                for key_name in keys_to_delete:
                    log.info("Deleting %s" % key_name)
                    s3.delete_object(Bucket=uri.bucket, Key=key_name)

        except s3.exceptions.NoSuchBucket:
            raise Exception("Invalid bucket: {}".format(uri.bucket))
        except botocore.exceptions.ClientError as e:
            # endpoint may also raise this for invalid bucket name
            if e.response["Error"]["Code"] == "InvalidBucketName":
                log.error("Invalid bucket: {}".format(uri.bucket))

            raise e


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
    path = fix_file(args.file)
    url = args.url

    if not os.path.exists(path):
        raise Exception("No such file or directory: {}".format(path))

    if os.path.isdir(path):
        raise Exception("FILE: %s is a directory. FILE must be a file." % path)

    log.info("Attempting to upload {}".format(path))

    # Validate URL
    uri = parse_uri(url)
    if uri.bucket is None:
        raise Exception("URL for put must have a bucket: %s" % url)
    if uri.key is None:
        uri.key = os.path.basename(path)

    config = get_config(args)

    # get s3 client with associated endpoint
    s3 = get_s3_client(config, uri)

    # Create the bucket if the user requested it and it does not exist
    if args.create_bucket:
        can_create = True
        try:
            s3.head_bucket(Bucket=uri.bucket)

            # no exception, we already own this bucket
            can_create = False
        except botocore.exceptions.ClientError as e:
            code = e.response["Error"]["Code"]

            if code != "404":
                log.error(code)
                raise e

        if can_create:
            s3.create_bucket(Bucket=uri.bucket)

    if not args.force:
        # check if all keys do not yet exist
        # accepted method of checking for existence of a key
        key_already_exists = False

        try:
            s3.head_object(Bucket=uri.bucket, Key=uri.key)
            key_already_exists = True
        except s3.exceptions.ClientError as e:
            code = e.response["ResponseMetadata"]["HTTPStatusCode"]

            if code != 404:
                log.error(code)
                raise e

        if key_already_exists:
            raise Exception(
                "Key: {} already exists. Try --force to overwrite".format(path)
            )

    try:
        key = path if uri.key is None else uri.key
        s3.upload_file(path, uri.bucket, key)
        log.info(
            "Uploaded file: {file} to bucket: {bucket} as key: {key}".format(
                file=path, bucket=uri.bucket, key=key
            )
        )
    except boto3.exceptions.S3UploadFailedError:
        raise Exception(
            "Failed to upload file: {file} to bucket: {bucket} as key: {key}".format(
                file=path, bucket=uri.bucket, key=key
            )
        )


def get(args):
    uri = parse_uri(args.url)

    if uri.bucket is None:
        raise Exception("URL must contain a bucket: %s" % args[0])
    if uri.key is None:
        raise Exception("URL must contain a key")

    if args.file:
        output = fix_file(args.file)
    else:
        output = os.path.basename(uri.key.rstrip("/"))

    log.info("Downloading %s" % uri)

    config = get_config(args)
    s3 = get_s3_client(config, uri)

    try:
        s3.download_file(Bucket=uri.bucket, Key=uri.key, Filename=output)
    except s3.exceptions.NoSuchBucket:
        raise Exception("Invalid bucket: {}".format(uri.bucket))
    except botocore.exceptions.ClientError as e:
        # endpoint may also raise this for invalid bucket name
        log.error("Response error code: {}".format(e.response["Error"]["Code"]))
        raise e

    log.info("Download: {} complete".format(uri))


# --- Handle Command Line Arguments --------------------------------------------
def parse_args(args):
    parser = ArgumentParser(prog="pegasus-s3")

    # add top level arguments
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose mode"
    )

    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")

    parser.add_argument(
        "-C", "--conf", dest="config", default=None, help="Path to configuration file"
    )

    # create a subparser that will handle s3 commands
    subparser = parser.add_subparsers(dest="cmd", help="Available s3 commands")

    # create subcommands for each of the supported s3 commands
    # LS command --------------------------
    parser_ls = subparser.add_parser("ls")
    parser_ls.add_argument("url", metavar="URL", help="URL to be looked up")
    parser_ls.add_argument(
        "-l",
        "--long",
        dest="long_format",
        action="store_true",
        default=False,
        help="Use long listing format",
    )
    parser_ls.add_argument(
        "-H",
        "--human-sized",
        dest="human_sized",
        action="store_true",
        default=False,
        help="Use human readable sizes",
    )
    parser_ls.set_defaults(func=ls)

    # MKDIR command -----------------------------
    parser_mkdir = subparser.add_parser("mkdir")
    parser_mkdir.add_argument(
        "url", metavar="URL", help="URL specifying bucket to be created"
    )
    parser_mkdir.set_defaults(func=mkdir)

    # RM command --------------------------
    parser_rm = subparser.add_parser("rm")
    parser_rm.add_argument(
        "url",
        metavar="URL",
        nargs="?",
        default=None,
        help="URL specifying key to be removed",
    )

    parser_rm.add_argument(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="Ignore nonexistent keys",
    )
    parser_rm.add_argument(
        "-F",
        "--file",
        dest="file",
        action="store",
        default=None,
        help="File containing a list of URLs to delete",
    )
    parser_rm.set_defaults(func=rm)

    # PUT command ---------------------------
    parser_put = subparser.add_parser("put")
    parser_put.add_argument("file", metavar="FILE", help="The file to be uploaded")

    parser_put.add_argument(
        "url", metavar="URL", help="URL to which the file will be uploaded"
    )

    parser_put.add_argument(
        "-b",
        "--create-bucket",
        dest="create_bucket",
        action="store_true",
        default=False,
        help="Create the destination bucket if it does not already exist",
    )

    parser_put.add_argument(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="Overwrite key if it already exists",
    )
    parser_put.set_defaults(func=put)

    # GET command --------------------------
    parser_get = subparser.add_parser("get")
    parser_get.add_argument("url", metavar="URL", help="URL of the key to download")

    parser_get.add_argument(
        "file",
        nargs="?",
        default=None,
        metavar="FILE",
        help="File that key will be downloaded as",
    )
    parser_get.set_defaults(func=get)

    # CP command -------------------------
    parser_cp = subparser.add_parser("cp")
    parser_cp.add_argument(
        "srcs", nargs="+", metavar="SRC", help="Sources to copy from"
    )

    parser_cp.add_argument("dest", metavar="DST", help="Destination to copy to")

    parser_cp.add_argument(
        "-c",
        "--create-dest",
        dest="create",
        action="store_true",
        default=False,
        help="Create destination bucket if it does not exist",
    )
    parser_cp.add_argument(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="If DEST key exists, then overwrite it",
    )
    parser_cp.set_defaults(func=cp)

    return parser, parser.parse_args(args)


# --- Entrypoint ---------------------------------------------------------------
def main():
    parser, args = parse_args(sys.argv[1:])

    configure_logging(args.verbose, args.debug)

    if args.cmd is None:
        parser.print_usage()
    else:
        try:
            args.func(args)
        except Exception as e:
            if args.verbose or args.debug:
                log.exception("Encountered error:")
            else:
                print(e)

            sys.exit(1)
