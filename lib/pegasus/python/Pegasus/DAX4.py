# TODO: licensing stuff
# TODO: example workflow for help(DAX4)
"""API for generating and running Pegasus workflows.
"""

__author__= "Pegasus Team"
__version__= "4.0"

import yaml
from collections import namedtuple

# TODO: decide which symbols to expose
#__all__ = []

# --- PEGASUS VERSION ----------------------------------------------------------
PEGASUS_VERSION = 5.0

# --- ERRORS -------------------------------------------------------------------
# TODO: document errors
class DAX4Error(Exception):
    pass

class DuplicateError(DAX4Error):
    pass

class NotFoundError(DAX4Error):
    pass

class FormatError(DAX4Error):
    pass

class ParseError(DAX4Error):
    pass

# --- YAML ---------------------------------------------------------------------
class YAMLAble:
    """
    Derived classes must implement functionality to return themselves as
    objects such that they can be used with PyYaml.dump(...)
    """
    def _YAMLify(self):
        """Return a a representation of self that can be dumped to yaml"""
        raise NotImplementedError

# --- Hooks --------------------------------------------------------------------
class EventType:
    """Event type on which a hook will be triggered"""
    NEVER = "never"
    START = "start"
    ERROR = "error"
    SUCCESS = "success"
    END = "end"
    ALL = "all"

class HookMixin(YAMLAble):
    """Derived class can have hooks assigned to it. This currently supports
    shell hooks, and will be extended to web hooks etc.
    """

    def __init__(self):
        """Constructor"""
        self.hooks = dict()
        self.hooks["shell"] = list()
    
    def add_shell_hook(self, event_type, cmd):
        """Add a shell hook
        
        :param event_type: an event type defined in DAX4.EventType
        :type event_type: str
        :param cmd: shell command
        :type cmd: str
        """
        self.hooks["shell"].append(ShellHook(event_type, cmd))
    
    def _YAMLify(self):
        yaml_obj = dict()

        '''
        group hooks together s.t. we have the following:
        {
            "shell": [
                {"_on": <EventType.xxxx>, "cmd": <shell command>},
                ...
            ]
        }
        '''
        for hook_type, items in self.hooks.items():
            yaml_obj[hook_type] = [hook._YAMLify() for hook in items]
        
        return yaml_obj

class Hook(YAMLAble):
    """Base class that specific hook types will inherit from"""
    def __init__(self, event_type):
        self.on = event_type

class ShellHook(Hook):
    """A hook that executes a shell command"""
    def __init__(self, event_type, cmd):
        Hook.__init__(self, event_type)
        self.cmd = cmd
    
    def _YAMLify(self):
        return {"_on": self.on, "cmd": self.cmd}

# --- Profiles -----------------------------------------------------------------
class Namespace:
    """
    Profile Namespace values recognized by Pegasus. See Executable,
    Transformation, and Job.
    """
    PEGASUS = "pegasus"
    CONDOR = "condor"
    DAGMAN = "dagman"
    ENV = "env"
    HINTS = "hints"
    GLOBUS = "globus"
    SELECTOR = "selector"
    STAT = "stat"

class ProfileMixin(YAMLAble):
    """Deriving class can have Profiles assigned to it"""
    
    # a profile entry 
    Profile = namedtuple("Profile", ["namespace", "key", "value"])

    def __init__(self):
        """Constructor"""
        self.profiles = set()

    def add_profile(self, namespace, key, value):
        """Add a profile to this object
        
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: profiles must be unique
        """
        p = ProfileMixin.Profile(namespace, key, value)
        if self.has_profile(namespace, key, value):
            raise DuplicateError("Duplicate profile %s" % p)
        self.profiles.add(p)

    def has_profile(self, namespace, key, value):
        """Check if a profile with the given namespace, key, and value exists
        
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: profiles must be unique
        :return: True if it exists, else false
        :rtype: bool
        """

        p = ProfileMixin.Profile(namespace, key, value)
        return p in self.profiles

    def remove_profile(self, namespace, key, value):
        """Remove profile from this object"""
        p = ProfileMixin.Profile(namespace, key, value)
        if not self.has_profile(p):
            raise NotFoundError("Profile not found", p)
        self.profiles.remove(p)

    def clear_profiles(self):
        """Remove all profiles from this object"""
        self.profiles.clear()

    def _YAMLify(self):
        yaml_obj = dict()

        '''
        group profiles together s.t. we have the following:
        {
            "ns1": {
                "key1": "value",
                "key2"" "value
            },
            "ns2": {...},
            ...
        }
        '''
        for p in self.profiles:
            if p.namespace not in yaml_obj:
                yaml_obj[p.namespace] = {p.key: p.value}
            else:
                yaml_obj[p.namespace][p.key] = p.value

        return yaml_obj

# --- Catalogs -----------------------------------------------------------------
class SiteCatalog:
    pass

class ReplicaCatalog(YAMLAble):
    """ReplicaCatalog class which maintains a mapping of logical filenames
    to physical filenames. This mapping is a one to many relationship.
    """

    # a replica catalog entry 
    Replica = namedtuple("Replica", ["lfn", "pfn", "site", "regex"])

    def __init__(self, filepath="ReplicaCatalog.yml"):
        """Constructor
        
        :param filepath: filepath to write this catalog to, defaults to "ReplicaCatalog.yml"
        :type filepath: str, optional
        """
        self.filepath = filepath
        self.replicas = set()
    
    def add_replica(self, lfn, pfn, site, regex=False):
        """Add an entry to the replica catalog
        
        :param lfn: logical filename
        :type lfn: str
        :param pfn: physical file name 
        :type pfn: str
        :param site: site at which this file resides
        :type site: str
        :param regex: whether or not the lfn is a regex pattern, defaults to False
        :type regex: bool, optional
        :raises DuplicateError: an entry with the same parameters already exists in the catalog
        """
        r = ReplicaCatalog.Replica(lfn, pfn, site, regex)
        if r in self.replicas:
            raise DuplicateError("Duplicate replica catalog entry {}".format(r))
        else:
            self.replicas.add(r)
    
    def write(self, filepath=""):
        """Write this catalog, formatted in YAML, to a file
        
        :param filepath: path to which this catalog will be written, defaults to self.filepath if filepath is "" or None
        :type filepath: str, optional
        """
        catalog = self._YAMLify()

        # when written out to a separate file, the catalog must contain
        # the pegasus api version 
        catalog["pegasus"] = PEGASUS_VERSION

        path = self.filepath if filepath == "" or filepath == None else filepath
        with open(path, "w") as file:
            yaml.dump(catalog, file)
            
    def _YAMLify(self):
        replicas_as_dicts = list()
        for e in self.replicas:
            replicas_as_dicts.append(
                {
                    "lfn": e.lfn,
                    "pfn": e.pfn,
                    "site": e.site,
                    "regex": e.regex
                }
            )
        return {"replicas": replicas_as_dicts}

class TransformationCatalog:
    pass

# --- Workflow -----------------------------------------------------------------

class AbstractJob:
    pass

class Job(AbstractJob):
    pass

class DAX(AbstractJob):
    pass

class Workflow:
    pass


