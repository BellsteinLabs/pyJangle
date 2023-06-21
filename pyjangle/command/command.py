import abc


class Command(metaclass = abc.ABCMeta):
    """Represents an intent to change the state of the system.
    
    Events are immutable objects that represent potential 
    state changes.  Ues names like "UpdateName" rather than
    "NameUpdated".  "NameUpdated" would be a good name for 
    #the corresponsing event.  The only requirement is that 
    a command can be mapped to an aggregate via an ID.
    
    It's also worth noting that when this class is extended,
    things like correlation ids and user ids are a good thing
    to tack on so that they can be passed to the corresponding 
    events that are created.  Makes for a great audit trail."""
    @abc.abstractmethod
    def get_aggregate_id(self):
        """An id used to associate the command to an aggregate."""
        pass