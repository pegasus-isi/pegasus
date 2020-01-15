from enum import Enum

from .errors import DuplicateError
from .errors import NotFoundError

# --- metadata -----------------------------------------------------------------
class MetadataMixin:
    """Derived class can have metadata assigned to it as key value pairs."""

    def add_metadata(self, key, value):
        """Add metadata as a key value pair to this object
        
        :param key: metadata key
        :type key: str
        :param value: metadata value
        :type value: str
        :raises DuplicateError: metadata keys must be unique
        :return: self
        """
        if key in self.metadata:
            raise DuplicateError

        self.metadata[key] = value

        return self

    def has_metadata(self, key):
        """Check if metadata with the given key exists for this object
        
        :param key: metadata key
        :type key: str
        :return: whether or not the given metadata key exists for this object
        :rtype: bool
        """
        return key in self.metadata

    def remove_metadata(self, key):
        """Remove a metadata key value pair
        
        :param key: metadata key
        :type key: str
        :raises NotFoundError: metadata key not found
        :return: self
        """
        if key not in self.metadata:
            raise NotFoundError

        del self.metadata[key]

        return self

    def clear_metadata(self):
        """Clear all the metadata assigned to this object
        
        :return: self
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
    shell hooks. The supported hooks are triggered when some event,
    specified by :py:class:`~Pegasus.dax4.mixins.EventType`, takes place.
    """

    def add_shell_hook(self, event_type, cmd):
        # TODO: consider making event_type either an event type or an actual ShellHook
        """Add a shell hook. The given command will be executed by the shell
        when the specified :py:class:`~Pegasus.dax4.mixins.EventType` takes
        place.

        .. code-block:: python

            # Example
            wf.add_shell_hook(EventType.START, "echo 'hello'")
        
        :param event_type: an event type defined in :py:class:`~Pegasus.dax4.mixins.EventType`
        :type event_type: EventType
        :param cmd: shell command
        :type cmd: str
        :raises ValueError: event_type must be one of :py:class:`~Pegasus.dax4.mixins.EventType`
        :return: self
        """
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.hooks[_ShellHook.__hook_type__].append(_ShellHook(event_type, cmd))

        return self


class _Hook:
    """Base class that specific hook types will inherit from"""

    def __init__(self, event_type):
        """Constructor
        
        :param event_type: an event type defined in :py:class:`~Pegasus.dax4.mixins.EventType`
        :type event_type: EventType
        :raises ValueError: event_type must be of type :py:class:`~Pegasus.dax4.mixins.EventType`
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
        
        :param event_type: an event type defined in :py:class:`~Pegasus.dax4.mixins.EventType`
        :type event_type: EventType
        :param cmd: shell command
        :type cmd: str
        """
        _Hook.__init__(self, event_type)
        self.cmd = cmd

    def __json__(self):
        return {"_on": self.on, "cmd": self.cmd}


# --- profiles -----------------------------------------------------------------
class Namespace(Enum):
    """Profile Namespace values recognized by Pegasus"""

    PEGASUS = "pegasus"
    CONDOR = "condor"
    DAGMAN = "dagman"
    ENV = "env"
    HINTS = "hints"
    GLOBUS = "globus"
    SELECTOR = "selector"
    STAT = "stat"


class ProfileMixin:
    """Derived class can have profiles assigned to it"""

    def add_profile(self, namespace, key, value):
        """Add a profile to this object
        
        .. code-block:: python

            # Example 1
            preprocess = (
                Transformation("preprocess")
                    .add_profile(Namespace.GLOBUS, "maxtime", 2)
                    .add_profile(Namespace.DAGMAN, "retry", 3)
            )

            # Example 2
            job = (
                Job(preprocess)
                    .add_profile(Namespace.ENV, "FOO", "bar")
            )

        :param namespace: a namespace defined in :py:class:`~Pegasus.dax4.mixins.Namespace`
        :type namespace: Namespace
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises ValueError: namespace must be one of :py:class:`~Pegasus.dax4.mixins.Namespace`
        :raises DuplicateError: profiles must be unique
        :return: self
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
        """Check if a profile with the given namespace, key, and value exists for
        this object.
        
        :param namespace: a namespace defined in :py:class:`~Pegasus.dax4.mixins.Namespace`
        :type namespace: Namespace
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises DuplicateError: profiles must be unique
        :raises ValueError: namespace must be one of :py:class:`~Pegasus.dax4.mixins.Namespace`
        :return: whether or not the given profile exists
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
        """
        self.profiles.clear()

        return self

