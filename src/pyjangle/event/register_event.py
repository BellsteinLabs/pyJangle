import inspect
from pyjangle import Event, LogToggles, log, JangleError

_name_to_event_type_map = dict()
__event_type_to_name_map = dict()


class EventRegistrationError(JangleError):
    "Decorated member is not an Event class."
    pass


class DuplicateEventNameRegistrationError(JangleError):
    "Name is already associated to an event."


def RegisterEvent(name: str = None):
    """decorates and registers an event class with an associated name.

    Registering an event with a name serves two purposes.  First, because the serialized
    event is stored with its name, the name suggests what type the event should be
    deserialized into.  Additionally, the name of the event can be useful when
    examining logs and records when troubleshooting.  If no name is provided,
    the default implementation is:

        type.__module__ + "." + type.__name__

    Some other examples of names you may find useful are:

        "com.example.events.WidgetCreated"
        "NameUpdated"

    Args:
        name:
            A name that is be associated to the event.

    Raises:
        EventRegistrationError:
            Decorated member is not an Event class.
        DuplicateEventNameRegistrationError:
            Name is already associated to an event.
    """

    def decorator(cls):
        global _name_to_event_type_map
        global __event_type_to_name_map
        event_name = ".".join([cls.__module__, cls.__name__]) if not name else name
        if not issubclass(cls, Event):
            raise EventRegistrationError("Decorated member is not an event")
        if (
            event_name in _name_to_event_type_map
            and _name_to_event_type_map[event_name] != cls
        ):
            raise DuplicateEventNameRegistrationError(
                "Name already registered",
                {
                    "name": event_name,
                    "current_registrant": str(_name_to_event_type_map[event_name]),
                    "duplicate_registrant": str(cls),
                },
            )
        _name_to_event_type_map[event_name] = cls
        __event_type_to_name_map[cls] = event_name
        log(
            LogToggles.event_registered,
            "Event registered",
            {"event_name": event_name, "event_type": str(cls)},
        )
        return cls

    if inspect.isclass(name):  # Decorator was used without parenthesis
        cls = name
        name = None
        return decorator(cls)
    return decorator


def get_event_type(name: str) -> type:
    """Returns the type registered to an event name.

    Names are registered to types with `RegisterEvent`.  This function returns the type
    associated to a name.

    Args:
        name:
            A name that has been associated to an event type.

    Returns:
        Type of the event associated to `name`.

    Raises:
        KeyError:
            Name is not associated to an event.
    """

    try:
        return _name_to_event_type_map[name]
    except KeyError:
        raise KeyError(
            f"""No event registered with name: {name}.  Ensure the event is decorated 
            with `RegisterEvent`."""
        )


def get_event_name(event_type: type) -> str:
    """Returns the name registered to an event type.

    Names are registered to types with `RegisterEvent`.  This function returns the name
    for a given event type.

    Args:
        event_type:
            An event type that has been associated to a name.

    Returns:
        The name associated to `event_type`.

    Raises:
        KeyError:
            `event_type` is not associated to a name.
    """
    try:
        return __event_type_to_name_map[event_type]
    except KeyError:
        raise KeyError(
            f"""{str(event_type)} is not registered as an event.  Ensure the event is 
            decorated with @RegisterEvent."""
        )
