from enum import Enum

from .Errors import DuplicateError, NotFoundError

'''
# --- metadata -----------------------------------------------------------------
class _MetadataMixin:
    def add_metadata(self, key, value):
        """Add metadata as a key value pair to this object
        
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: metadata keys must be unique
        """
        if key in self.metadata:
            raise DuplicateError
        else:
            self.metadata[key] = value

    def update_metadata(self, key, value):
        """Update metadata
        
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises NotFoundError: key not found 
        """
        if key not in self.metadata:
            raise NotFoundError
        else:
            self.metadata[key] = value

    def has_metadata(self, key):
        """Check if metadata with the given key exists for this object
        
        :param key: key
        :type key: str
        :return: whether or not the given metadata key exists for this object
        :rtype: bool
        """
        return key in self.metadata

    def clear_metadata(self):
        """Clear all the metadata given to this object"""
        self.metadata.clear()


# --- hooks --------------------------------------------------------------------
class EventType(Enum):
    """Event type on which a hook will be triggered"""

    NEVER = "never"
    START = "start"
    ERROR = "error"
    SUCCESS = "success"
    END = "end"
    ALL = "all"


class _HookMixin:
    """Derived class can have hooks assigned to it. This currently supports
    shell hooks, and will be extended to web hooks etc.
    """

    def __init__(self):
        """Constructor"""
        self.hooks = defaultdict()

        # num hooks as if self.hooks was flattened
        self.num_hooks = 0

    def add_shell_hook(self, event_type, cmd):
        """Add a shell hook
        
        :param event_type: an event type defined in DAX4.EventType
        :type event_type: str
        :param cmd: shell command
        :type cmd: str
        """
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.hooks[ShellHook.__hook_type__].append(ShellHook(event_type.value, cmd))
        self.num_hooks += 1

    def __len__(self):
        return self.num_hooks

    def _YAMLify(self):
        yaml_obj = dict()

        """
        group hooks together s.t. we have the following:
        {
            "shell": [
                {"_on": <EventType.xxxx>, "cmd": <shell command>},
                ...
            ]
        }
        """
        for hook_type, items in self.hooks.items():
            yaml_obj[hook_type] = [hook._YAMLify() for hook in items]

        return yaml_obj


class Hook(_YAMLAble):
    """Base class that specific hook types will inherit from"""

    def __init__(self, event_type):
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.on = event_type.value


class ShellHook(Hook):
    """A hook that executes a shell command"""

    __hook_type__ = "shell"

    def __init__(self, event_type, cmd):
        Hook.__init__(self, event_type.value)
        self.cmd = cmd

    def _YAMLify(self):
        return {"_on": self.on, "cmd": self.cmd}


'''
# --- profiles -----------------------------------------------------------------
class Namespace(Enum):
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


class ProfileMixin:
    """Deriving class can have Profiles assigned to it"""

    def add_profile(self, namespace, key, value):
        """Add a profile to this object
        
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: profiles must be unique
        :return: self
        :rtype: type(self)
        """
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        if namespace.value in self.profiles:
            if key in self.profiles[namespace.value]:
                raise DuplicateError(
                    "Duplicate profile with namespace: {0}, key: {1}, value: {2}".format(
                        namespace.value, key, value
                    )
                )

        self.profiles[namespace.value][key] = value

        return self

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
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        is_found = False
        if namespace.value in self.profiles:
            if key in self.profiles[namespace.value]:
                is_found = True

        return is_found

    def remove_profile(self, namespace, key, value):
        """Remove a profile from this object
        
        :param namespace: a namespace defined in DAX4.Namespace
        :type namespace: str (defined in DAX4.Namespace)
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises ValueError: Namespace must be one of DAX4.Namespace
        :raises NotFoundError: given profile with namespace key and value is not found 
        :return: self
        :rtype: type(self)
        """
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        if not self.has_profile(namespace, key, value):
            raise NotFoundError(
                "Profile with namespace: {}, key: {}, value: {} not found".format(
                    namespace.value, key, value
                )
            )

        del self.profiles[namespace.value][key]

        # Removing namespaces with no k,v pairs
        if len(self.profiles[namespace.value]) == 0:
            del self.profiles[namespace.value]

        return self

    def clear_profiles(self):
        """Remove all profiles from this object
        
        :return: self
        :rtype: type(self)
        """
        self.profiles.clear()

        return self

