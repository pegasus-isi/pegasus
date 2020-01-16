from enum import Enum

from .errors import DuplicateError
from .errors import NotFoundError

# --- metadata -----------------------------------------------------------------
class MetadataMixin:
    """Derived class can have metadata assigned to it as key value pairs."""

    def add_metadata(self, key, value):
        """Add metadata as a key value pair to this object
        
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: metadata keys must be unique
        :return: self
        :rtype: object type that uses MetadataMixin
        """
        if key in self.metadata:
            raise DuplicateError

        self.metadata[key] = value

        return self

    def has_metadata(self, key):
        """Check if metadata with the given key exists for this object
        
        :param key: key
        :type key: str
        :return: whether or not the given metadata key exists for this object
        :rtype: bool
        """
        return key in self.metadata

    def remove_metadata(self, key):
        """Remove a metadata key value pair
        
        :param key: key
        :type key: str
        :raises NotFoundError: key not found
        :return: self
        :rtype: object type that uses MetadataMixin
        """
        if key not in self.metadata:
            raise NotFoundError

        del self.metadata[key]

        return self

    def clear_metadata(self):
        """Clear all the metadata given to this object
        
        :return: self
        :rtype: object type that uses MetadataMixin
        """
        self.metadata.clear()

        return self


# --- hooks --------------------------------------------------------------------
class EventType(Enum):
    """Event type on which a hook will be triggered"""

    NEVER = "never"
    START = "start"
    ERROR = "error"
    SUCCESS = "success"
    END = "end"
    ALL = "all"


class HookMixin:
    """Derived class can have hooks assigned to it. This currently supports
    shell hooks, and will be extended to web hooks etc.
    """

    def add_shell_hook(self, event_type, cmd):
        # TODO: consider making event_type either an event type or an actual ShellHook
        """Add a shell hook
        
        :param event_type: an event type defined in DAX4.EventType
        :type event_type: str
        :param cmd: shell command
        :type cmd: str
        """
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.hooks[_ShellHook.__hook_type__].append(_ShellHook(event_type, cmd))

        return self


class _Hook:
    """Base class that specific hook types will inherit from"""

    def __init__(self, event_type):
        """Constructor
        
        :param event_type: one of EventType
        :type event_type: EventType
        :raises ValueError: event_type must be one of EventType
        """
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.on = event_type.value

    # TODO: def get/set event type


# TODO: make this public
class _ShellHook(_Hook):
    """A hook that executes a shell command"""

    __hook_type__ = "shell"

    def __init__(self, event_type, cmd):
        """Constructor
        
        :param event_type: one of EventType
        :type event_type: EventType
        :param cmd: shell command to be executed
        :type cmd: str
        """
        _Hook.__init__(self, event_type)
        self.cmd = cmd

    def __json__(self):
        return {"_on": self.on, "cmd": self.cmd}


# --- profiles -----------------------------------------------------------------
class Namespace(Enum):
    """
    Profile Namespace values recognized by Pegasus. See Transformation, and Job.
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
                if self.profiles[namespace.value][key] == value:
                    is_found = True

        return is_found

    '''
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
    '''

    def clear_profiles(self):
        """Remove all profiles from this object
        
        :return: self
        :rtype: type(self)
        """
        self.profiles.clear()

        return self

