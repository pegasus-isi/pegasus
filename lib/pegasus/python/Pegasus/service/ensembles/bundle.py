import os
import zipfile

from Pegasus.tools import properties

PROPERTIES_NAME = "pegasus.properties"


class BundleException(Exception):
    pass


class Bundle:
    def __init__(self, filename):
        self.filename = filename

        if not os.path.isfile(filename):
            raise BundleException("No such file")

        # Verify that the bundle is a valid zip file
        if not zipfile.is_zipfile(filename):
            raise BundleException("Bundle is not a valid zip file")

        # open the bundle
        self.zipfile = zipfile.ZipFile(filename, "r")

        # Verify that the bundle does not contain any harmful paths
        for name in self.zipfile.namelist():
            if name.startswith("/") or ".." in name:
                raise BundleException("Invalid bundle entry: %s" % name)

        # Verify that the bundle contains a manifest file
        if not self.contains(PROPERTIES_NAME):
            raise BundleException("Bundle does not contain %s" % PROPERTIES_NAME)

        # Open the properties entry
        self.properties = properties.parse_properties(
            self.zipfile.open(PROPERTIES_NAME, "r")
        )

    def contains(self, name):
        # getinfo raises a KeyError if the entry does not exist
        try:
            self.zipfile.getinfo(name)
            return True
        except KeyError:
            return False

    def _ensure_valid_file(self, propname):
        filename = self.properties.get(propname)
        if filename is None:
            return

        if not self.contains(filename) and not os.path.isfile(filename):
            raise BundleException(
                "Invalid {}: No such file: {}".format(propname, filename)
            )

    def verify(self):
        # Make sure the dax property is set
        daxfile = self.properties.get("pegasus.dax.file")
        if daxfile is None:
            raise BundleException("Bundle properties does not contain pegasus.dax.file")

        # Make sure these file properties, if they are set, point to valid
        # files in either the zip file or the file system
        self._ensure_valid_file("pegasus.dax.file")
        self._ensure_valid_file("pegasus.catalog.replica.file")
        self._ensure_valid_file("pegasus.catalog.transformation.file")
        self._ensure_valid_file("pegasus.catalog.site.file")

    def get_properties(self):
        return self.properties

    def unpack(self, dirname):
        self.zipfile.extractall(dirname)
