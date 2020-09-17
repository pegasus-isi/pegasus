import logging
import os
import unittest
from configparser import ConfigParser
from pathlib import Path

import boto3
import botocore
import pytest

from Pegasus import s3


class TestPaths(unittest.TestCase):
    def test_get_path_for_key(self):
        # s3.get_path_for_key(bucket, searchkey, key, output)
        self.assertEqual(s3.get_path_for_key("bucket", None, "foo", "baz"), "baz/foo")
        self.assertEqual(
            s3.get_path_for_key("bucket", None, "foo", "baz/"), "baz/bucket/foo"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", None, "foo/bar", "baz"), "baz/foo/bar"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", None, "foo/bar", "baz/"), "baz/bucket/foo/bar"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo", "foo/bar", "baz"), "baz/bar"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo", "foo/bar", "baz/"), "baz/foo/bar"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo", "foo/bar/boo", "baz"), "baz/bar/boo"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo/", "foo/bar", "baz"), "baz/bar"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo/", "foo/bar", "baz/"), "baz/foo/bar"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo/bar", "foo/bar", "baz"), "baz"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo/bar", "foo/bar/boo", "baz"), "baz/boo"
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo/bar", "foo/bar/boo", "baz/"),
            "baz/bar/boo",
        )
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo/bar", "foo/bar/boo/moo/choo", "baz/"),
            "baz/bar/boo/moo/choo",
        )
        self.assertEqual(s3.get_path_for_key("bucket", None, "foo/", "baz"), "baz/foo")
        self.assertEqual(s3.get_path_for_key("bucket", None, "foo", ""), "foo")
        self.assertEqual(s3.get_path_for_key("bucket", None, "foo/", ""), "foo")
        self.assertEqual(s3.get_path_for_key("bucket", None, "foo/bar", ""), "foo/bar")
        self.assertEqual(s3.get_path_for_key("bucket", "foo", "foo/", "baz"), "baz")
        self.assertEqual(s3.get_path_for_key("bucket", "foo/", "foo/", "baz"), "baz")
        self.assertEqual(
            s3.get_path_for_key("bucket", "foo", "foo/bar/", "baz"), "baz/bar"
        )

    def test_get_key_for_path(self):
        # s3.get_key_for_path(infile, path, outkey):
        self.assertEqual(s3.get_key_for_path("/foo", "/foo", "baz/"), "baz/foo")
        self.assertEqual(s3.get_key_for_path("/foo/", "/foo", "baz/"), "baz/foo")
        self.assertEqual(s3.get_key_for_path("/foo/", "/foo/", "baz/"), "baz/foo")
        self.assertEqual(s3.get_key_for_path("/foo", "/foo/", "baz/"), "baz/foo")
        self.assertEqual(s3.get_key_for_path("/foo", "/foo/bar", "baz"), "baz/bar")
        self.assertEqual(s3.get_key_for_path("/foo", "/foo/bar", "baz/"), "baz/foo/bar")
        self.assertEqual(
            s3.get_key_for_path("/foo", "/foo/bar/baz", "baz/"), "baz/foo/bar/baz"
        )
        self.assertEqual(
            s3.get_key_for_path("/foo", "/foo/bar/baz", "baz"), "baz/bar/baz"
        )
        self.assertEqual(
            s3.get_key_for_path("/foo", "/foo/bar/baz", "baz/boo/"),
            "baz/boo/foo/bar/baz",
        )
        self.assertRaises(Exception, s3.get_key_for_path, "/foo", "/foo/bar/baz", "")
        self.assertRaises(Exception, s3.get_key_for_path, "foo", "foo", "bar")
        self.assertRaises(Exception, s3.get_key_for_path, "/foo", "/bar", "bar")


# --- testing pegasus-s3 commands ----------------------------------------------
@pytest.fixture(scope="module")
def s3_client():
    CFG_PATH = (Path.home() / ".pegasus/credentials.conf").resolve()
    os.environ["S3CFG"] = str(CFG_PATH)
    cfg = ConfigParser()
    if len(cfg.read(str(CFG_PATH))) != 1:
        raise RuntimeError("Could not find {}".format(CFG_PATH))

    return boto3.client(
        "s3",
        endpoint_url=cfg["osgconnect"]["endpoint"],
        aws_access_key_id=cfg["rynge@osgconnect"]["access_key"],
        aws_secret_access_key=cfg["rynge@osgconnect"]["secret_key"],
    )


# for use in teardown functions as we cannot reuse the s3_client fixture there
def get_s3_client():
    CFG_PATH = (Path.home() / ".pegasus/credentials.conf").resolve()
    os.environ["S3CFG"] = str(CFG_PATH)
    cfg = ConfigParser()
    if len(cfg.read(str(CFG_PATH))) != 1:
        raise RuntimeError("Could not find {}".format(CFG_PATH))

    return boto3.client(
        "s3",
        endpoint_url=cfg["osgconnect"]["endpoint"],
        aws_access_key_id=cfg["rynge@osgconnect"]["access_key"],
        aws_secret_access_key=cfg["rynge@osgconnect"]["secret_key"],
    )


@pytest.fixture(scope="function")
def test_file():
    test_file = Path("test_file")
    with test_file.open("w") as f:
        f.write("sample text")

    yield test_file

    test_file.unlink()


# assumption is that .pegasus/credentials.conf exists with proper configuration
def is_missing_credentials():
    try:
        CFG_PATH = (Path.home() / ".pegasus/credentials.conf").resolve()
    except FileNotFoundError:
        return True

    cfg = ConfigParser()
    if len(cfg.read(str(CFG_PATH))) != 1:
        return True

    if "osgconnect" not in cfg and "rynge@osgconnect" not in cfg:
        return True

    return False


class TestLs:
    def teardown_method(self, method):
        s3_client = get_s3_client()

        # cleanup in case test fails
        try:
            s3_client.delete_object(Bucket="test-bucket", Key="test_file")
            s3_client.delete_bucket(Bucket="test-bucket")
        except s3_client.exceptions.NoSuchBucket:
            pass

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_ls_buckets(self, s3_client, capsys):
        resp = s3_client.list_buckets()
        expected_buckets = {b["Name"] for b in resp["Buckets"]}

        # pegasus-s3 ls s3://rynge@osgconnect
        parser, args = s3.parse_args(["ls", "s3://rynge@osgconnect"])
        s3.ls(args)

        # get stdout
        out, _ = capsys.readouterr()

        retrieved_buckets = {b.strip() for b in out.split()}
        assert expected_buckets == retrieved_buckets

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_ls_keys(self, s3_client, test_file, capsys):
        BUCKET = "test-bucket"

        # create test bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # add test file to bucket
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # pegasus-s3 ls s3://rynge@osgconnect/test-bucket
        parser, args = s3.parse_args(["ls", "s3://rynge@osgconnect/test-bucket"])
        s3.ls(args)

        # get stdout
        out, _ = capsys.readouterr()

        # test
        assert out.strip() == test_file.name

        # cleanup bucket
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)


class TestCp:
    def teardown_method(self, method):
        s3_client = get_s3_client()

        # cleanup in case test fails
        try:
            s3_client.delete_object(Bucket="test-bucket", Key="test_file")
            s3_client.delete_object(Bcuekt="test-bucket2", Key="test_file")
            s3_client.delete_bucket(Bucket="test-bucket")
            s3_client.delete_bucket(Bucket="test-bucket2")
        except s3_client.exceptions.NoSuchBucket:
            pass

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_cp(self, s3_client, test_file):
        BUCKET = "test-bucket"
        BUCKET2 = "test-bucket2"

        # create test buckets
        s3_client.create_bucket(Bucket=BUCKET)
        s3_client.create_bucket(Bucket=BUCKET2)

        # add test file to bucket
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # pegasus-s3 cp s3://rynge@osgconnect/test-bucket/test_file s3://rynge@osgconnect/test-bucket2/test_file
        parser, args = s3.parse_args(
            [
                "cp",
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET2, test_file.name),
            ],
        )
        s3.cp(args)

        # ensure BUCKET2 has been created and test_file copied
        resp = s3_client.list_buckets()
        expected_buckets = {b["Name"] for b in resp["Buckets"]}

        assert BUCKET2 in expected_buckets

        try:
            s3_client.head_object(Bucket=BUCKET2, Key=test_file.name)
        except sbotocore.exceptions.ClientError:
            pytest.fail(
                "s3://rynge@osgconnect/{}/{} should exist but does not".format(
                    BUCKET2, test_file.name
                )
            )

        # clean up buckets
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)
        s3_client.delete_object(Bucket=BUCKET2, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET2)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_cp_force(self, s3_client, test_file):
        BUCKET = "test-bucket"
        BUCKET2 = "test-bucket2"

        # create test buckets
        s3_client.create_bucket(Bucket=BUCKET)
        s3_client.create_bucket(Bucket=BUCKET2)

        # add test file to buckets
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)
        s3_client.upload_file(str(test_file), BUCKET2, test_file.name)

        # pegasus-s3 cp --force s3://rynge@osgconnect/test-bucket/test_file s3://rynge@osgconnect/test-bucket2/test_file
        parser, args = s3.parse_args(
            [
                "cp",
                "--force",
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET2, test_file.name),
            ],
        )
        s3.cp(args)

        # ensure test_file copied into bucket
        try:
            s3_client.head_object(Bucket=BUCKET2, Key=test_file.name)
        except sbotocore.exceptions.ClientError:
            pytest.fail(
                "s3://rynge@osgconnect/{}/{} should exist but does not".format(
                    BUCKET2, test_file.name
                )
            )

        # clean up buckets
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)
        s3_client.delete_object(Bucket=BUCKET2, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET2)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_cp_create_dest(self, s3_client, test_file):
        BUCKET = "test-bucket"
        BUCKET2 = "test-bucket2"

        # create test buckets
        s3_client.create_bucket(Bucket=BUCKET)

        # add test file to buckets
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # pegasus-s3 cp --create-dest s3://rynge@osgconnect/test-bucket/test_file s3://rynge@osgconnect/test-bucket2/test_file
        parser, args = s3.parse_args(
            [
                "cp",
                "--create-dest",
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET2, test_file.name),
            ],
        )
        s3.cp(args)

        # ensure BUCKET2 has been created and test_file copied
        resp = s3_client.list_buckets()
        expected_buckets = {b["Name"] for b in resp["Buckets"]}

        assert BUCKET2 in expected_buckets

        try:
            s3_client.head_object(Bucket=BUCKET2, Key=test_file.name)
        except sbotocore.exceptions.ClientError:
            pytest.fail(
                "s3://rynge@osgconnect/{}/{} should exist but does not".format(
                    BUCKET2, test_file.name
                )
            )

        # clean up buckets
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)
        s3_client.delete_object(Bucket=BUCKET2, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET2)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_cp_create_dest_and_force(self, s3_client, test_file):
        BUCKET = "test-bucket"
        BUCKET2 = "test-bucket2"

        # create test bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # add test file to bucket
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # pegasus-s3 cp --create-dest --force s3://rynge@osgconnect/test-bucket/test_file s3://rynge@osgconnect/test-bucket2/test_file
        parser, args = s3.parse_args(
            [
                "cp",
                "--create-dest",
                "--force",
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET2, test_file.name),
            ],
        )
        s3.cp(args)

        # ensure BUCKET2 has been created an test_file copied
        resp = s3_client.list_buckets()
        expected_buckets = {b["Name"] for b in resp["Buckets"]}

        assert BUCKET2 in expected_buckets

        try:
            s3_client.head_object(Bucket=BUCKET2, Key=test_file.name)
        except sbotocore.exceptions.ClientError:
            pytest.fail(
                "s3://rynge@osgconnect/{}/{} should exist but does not".format(
                    BUCKET2, test_file.name
                )
            )

        # clean up buckets
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)
        s3_client.delete_object(Bucket=BUCKET2, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET2)


class TestMkdir:
    def teardown_method(self, method):
        s3_client = get_s3_client()

        # cleanup in case test fails
        try:
            s3_client.delete_bucket(Bucket="test-bucket")
        except s3_client.exceptions.NoSuchBucket:
            pass

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_mkdir(self, s3_client):
        BUCKET = "test-bucket"

        # pegasus-s3 mkdir s3://rynge@osgconnect/test-bucket
        parser, args = s3.parse_args(
            ["mkdir", "s3://rynge@osgconnect/{}".format(BUCKET)]
        )
        s3.mkdir(args)

        # ensure test-bucket has been created
        resp = s3_client.list_buckets()
        expected_buckets = {b["Name"] for b in resp["Buckets"]}
        assert BUCKET in expected_buckets

        # cleanup test-bucket
        s3_client.delete_bucket(Bucket=BUCKET)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_mkdir_bucket_already_owned_by_user(self, s3_client, caplog):
        BUCKET = "test-bucket"

        # create test bucket before calling mkdir on same bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # pegasus-s3 mkdir s3://rynge@osgconnect/test-bucket
        parser, args = s3.parse_args(
            ["mkdir", "s3://rynge@osgconnect/{}".format(BUCKET)]
        )
        s3.mkdir(args)

        # check that log message printed
        assert caplog.record_tuples == [
            (
                "root",
                logging.WARNING,
                "Bucket: test-bucket exists and is already owned by user: rynge@osgconnect",
            )
        ]

        # cleanup test-bucket
        s3_client.delete_bucket(Bucket=BUCKET)


class TestRm:
    def teardown_method(self, method):
        s3_client = get_s3_client()

        # cleanup in case test fails
        try:
            s3_client.delete_object(Bucket="test-bucket", Key="test_file")
            s3_client.delete_bucket(Bucket="test-bucket")

            s3_client.delete_object(Bucket="test-bucket1", Key="tb1_f1")
            s3_client.delete_object(Bucket="test-bucket1", Key="tb1_f2")
            s3_client.delete_bucket(Bucket="test-bucket1")

            s3_client.delete_object(Bucket="test-bucket2", Key="tb2_f1")
            s3_client.delete_object(Bucket="test-bucket2", Key="tb2_f2")
            s3_client.delete_bucket(Bucket="test-bucket2")
        except s3_client.exceptions.NoSuchBucket:
            pass

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_rm(self, s3_client, test_file):
        BUCKET = "test-bucket"

        # create test bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # add test file to bucket
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # ensure file is there before test
        try:
            s3_client.head_object(Bucket=BUCKET, Key=test_file.name)
        except botocore.exceptions.ClientError:
            pytest.fail(
                "Failed to upload test file: {} to bucket: {}".format(test_file, BUCKET)
            )

        # remove file
        # pegasus-s3 rm s3://rynge@osgconnect/test-bucket/test_file
        parser, args = s3.parse_args(
            ["rm", "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name)]
        )
        s3.rm(args)

        try:
            s3_client.head_object(Bucket=BUCKET, Key=test_file.name)
        except botocore.exceptions.ClientError:
            # removed key and it should not exist
            pass

        # cleanup bucket
        s3_client.delete_bucket(Bucket=BUCKET)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_rm_force(self, s3_client, test_file):
        BUCKET = "test-bucket"

        # create test bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # ensure file is NOT there before test
        try:
            s3_client.head_object(Bucket=BUCKET, Key=test_file.name)
            pytest.fail(
                "File: {} should not be in bucket: {}".format(test_file.name, BUCKET)
            )
        except botocore.exceptions.ClientError:
            pass

        # remove file
        # pegasus-s3 rm s3://rynge@osgconnect/test-bucket/test_file
        parser, args = s3.parse_args(
            [
                "rm",
                "--force",
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
            ]
        )
        s3.rm(args)

        try:
            s3_client.head_object(Bucket=BUCKET, Key=test_file.name)
        except botocore.exceptions.ClientError:
            # removed key and it should not exist
            pass

        # cleanup bucket
        s3_client.delete_bucket(Bucket=BUCKET)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_rm_using_file(self, s3_client):
        files = [Path("tb1_f1"), Path("tb1_f2"), Path("tb2_f1"), Path("tb2_f2")]

        # create test files
        for f in files:
            with f.open("w") as fp:
                fp.write(f.name)

        # mapping of keys to buckets
        buckets_keys = {"test-bucket1": files[0:2], "test-bucket2": files[2:]}

        # create test buckets and upload files
        for b, keys in buckets_keys.items():
            s3_client.create_bucket(Bucket=b)
            for k in keys:
                s3_client.upload_file(str(k), b, k.name)

        # create rm file
        rm_file = Path("rm_file")
        with rm_file.open("w") as fp:
            for b, keys in buckets_keys.items():
                for k in keys:
                    fp.write("s3://rynge@osgconnect/{}/{}\n".format(b, k.name))

        # remove file
        # pegasus-s3 rm s3://rynge@osgconnect/test-bucket/test_file
        parser, args = s3.parse_args(["rm", "--force", "--file", rm_file.name])
        s3.rm(args)

        # ensure keys were removed from bucket
        for b, keys in buckets_keys.items():
            for k in keys:
                try:
                    s3_client.head_object(Bucket=b, Key=k.name)
                    pytest.fail(
                        "Key: {} should not exist in Bucket: {}".format(k.name, b)
                    )
                except botocore.exceptions.ClientError:
                    pass

        # cleanup buckets
        for b, keys in buckets_keys.items():
            for k in keys:
                s3_client.delete_object(Bucket=b, Key=k.name)
            s3_client.delete_bucket(Bucket=b)

        # remove test files
        rm_file.unlink()
        for f in files:
            f.unlink()


class TestPut:
    def teardown_method(self, method):
        s3_client = get_s3_client()

        # cleanup in case test fails
        try:
            s3_client.delete_object(Bucket="test-bucket", Key="test_file")
            s3_client.delete_bucket(Bucket="test-bucket")
        except s3_client.exceptions.NoSuchBucket:
            pass

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_put(self, s3_client, test_file):
        BUCKET = "test-bucket"

        # create test bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # pegasus-s3 put test_file s3://rynge@osgconnect/test-bucket/test_file
        paser, args = s3.parse_args(
            [
                "put",
                str(test_file),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
            ],
        )
        s3.put(args)

        # test
        try:
            s3_client.head_object(Bucket=BUCKET, Key=test_file.name)
        except botocore.exceptions.ClientError:
            pytest.fail(
                "Key: {} should exist in Bucket: {}".format(test_file.name, BUCKET)
            )

        # cleanup bucket
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_put_bucket_doesnt_exist(self, test_file):
        BUCKET = "test-bucket"

        # pegasus-s3 put test_file s3://rynge@osgconnect/test-bucket/test_file
        parser, args = s3.parse_args(
            [
                "put",
                str(test_file),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
            ]
        )

        with pytest.raises(Exception) as e:
            s3.put(args)

        assert "Failed to upload file: test_file" in str(e)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_put_create_bucket(self, s3_client, test_file):
        BUCKET = "test-bucket"

        # ensure test bucket doesn't yet exist
        try:
            s3_client.head_bucket(Bucket=BUCKET)
            pytest.fail(
                "test-bucket should not yet exist as it is to be created by the put command"
            )
        except botocore.exceptions.ClientError:
            pass

        # pegasus-s3 put --create-bucket test_file s3://rynge@osgconnect/test-bucket/test_file
        parser, args = s3.parse_args(
            [
                "put",
                "--create-bucket",
                str(test_file),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
            ]
        )
        s3.put(args)

        # test
        try:
            s3_client.head_object(Bucket=BUCKET, Key=test_file.name)
        except botocore.exceptions.ClientError:
            pytest.fail(
                "Key: {} should exist in Bucket: {}".format(test_file.name, BUCKET)
            )

        # cleanup
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_put_key_already_exists(self, s3_client, test_file):
        BUCKET = "test-bucket"

        # create bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # place file in bucket
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # upload file (key that already exists in bucket)
        # pegasus-s3 put test_file s3://rynge@osgconnect/test-bucket/test_file
        paser, args = s3.parse_args(
            [
                "put",
                str(test_file),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
            ]
        )

        with pytest.raises(Exception) as e:
            s3.put(args)

        assert "Key: test_file already exists" in str(e)

        # cleanup
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_put_force(self, s3_client, test_file):
        BUCKET = "test-bucket"

        # create bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # place file in bucket so that pegasus-s3 put can overwrite it
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # upload file
        # pegasus-s3 put --force test_file s3://rynge@osgconnect/test-bucket/test_file
        parser, args = s3.parse_args(
            [
                "put",
                "--force",
                str(test_file),
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
            ]
        )
        s3.put(args)

        try:
            s3_client.head_object(Bucket=BUCKET, Key=test_file.name)
        except botocore.exceptions.ClientError:
            pytest.fail(
                "Key: {} should exist in Bucket: {}".format(test_file.name, BUCKET)
            )

        # cleanup
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)


class TestGet:
    def teardown_method(self, method):
        s3_client = get_s3_client()

        # cleanup in case test fails
        try:
            s3_client.delete_object(Bucket="test-bucket", Key="test_file")
            s3_client.delete_bucket(Bucket="test-bucket")
        except s3_client.exceptions.NoSuchBucket:
            pass

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_get(self, s3_client):
        BUCKET = "test-bucket"

        test_file = Path("test_file")
        with test_file.open("w") as f:
            f.write("sample text\n")

        # create bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # place file in bucket so that pegasus-s3 put can overwrite it
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # remove file
        test_file.unlink()

        # get file
        # pegasus-s3 get s3://rynge@osgconnect/test-bucket/test_file
        parser, args = s3.parse_args(
            ["get", "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name)]
        )
        s3.get(args)

        # file should exist again
        assert test_file.exists() == True
        test_file.unlink()

        # cleanup
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)

    @pytest.mark.skipif(is_missing_credentials(), reason="missing credentials")
    def test_get_as_file(self, s3_client, test_file):
        BUCKET = "test-bucket"

        # create bucket
        s3_client.create_bucket(Bucket=BUCKET)

        # place file in bucket
        s3_client.upload_file(str(test_file), BUCKET, test_file.name)

        # get file
        # pegasus-s3 get s3://rynge@osgconnect/test-bucket/test_file new_test_file
        parser, args = s3.parse_args(
            [
                "get",
                "s3://rynge@osgconnect/{}/{}".format(BUCKET, test_file.name),
                "new_test_file",
            ]
        )
        s3.get(args)

        # check that test_file downloaded as new_test_file
        new_test_file = Path("new_test_file")
        assert new_test_file.exists() == True

        # cleanup
        new_test_file.unlink()
        s3_client.delete_object(Bucket=BUCKET, Key=test_file.name)
        s3_client.delete_bucket(Bucket=BUCKET)


if __name__ == "__main__":
    unittest.main()
