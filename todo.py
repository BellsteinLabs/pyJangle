#create hook for json dumps encoders
#Example scenario
#sql
#asyncio
#event replay
#Can register multiple query handlers
#remove type from saga metadata
#Fix uniqueness constraints on saga store
#what type of event does reconstitutesagastate use?
#saga should short on timeout or iscomplete

#handle and think through marking events as completed for event handlers and sagas
#Create concrete things needed to make this go
#add logging
#async event handlers


#handle case where events are missing
#given an intent
#  find the relevant intentHandler
#  based on the result of the intent Handler, return whatever result is relevant

#given a command, get a snapshot and lookup the relevant events
#  Use the events to build a state  **events should be playable in ANY order, and duplicate events should be tolerated**
#  Determine if the command is valid and create the resulting events
#  Dispatch the events to the event store
#  If fail: get new events the designated number of times before quitting
#  If succeed: proceed
#  Update the current state and create a new snapshot


#Event Handlers
#  Get an event from the stream
#  Apply the event to relevant views where the event is still applicable (version is appropriate)
#  If Fail: 
#   Put the event on a failed events queue and let someone know
#  

#Event Handler retry daemon
#  Wakeup every X seconds process failed events queue
#  If succeed, let someone know

#Fun metrics
#  State creation from events & snapshots
#  Size of failed events queue
#  commands processed per minute
#  events created per minute

#Events
#ID uuid (DO NOT USE THIS AS A CLUSTERING KEY!!!!)



#Event Store Schema
#  SequentialId             int         NOTNULL, CLUSTERED, AUTO-INC
#  AggregateID              GUID        PK, NOTNULL, NOT CLUSTERED
#  Version                  int         PK, NOTNULL
#  TraceID                  GUID        NOTNULL
#  CreatedFrom              int         NOTNULL (Ex: Command, Saga, etc)
#  USERID                   string      NOTNULL (SOMETHING caused the event to happen whether it's a user/service/daemon)
#  SerializationMetadata    string      NOTNULL
#  Data                     string      NOTNULL
#  CreatedAt                Datetime    NOTNULL, AUTO





#FEATURES
# Idempotent Event Handling
# Stateless/Horizontally Scalable

# Not Accounted For
#   How are events marked as "handled"
#       Have a shared location where the latest handled version is posted

#the only time saga state should be modified is in reconstitute_saga_state methods
#sagas and aggregates dont directly depend on services--they listen for events!  Call the service and transform the response into an event!
#don't put things in event payload that can change and cause the event to be invalid
#any validation that can be done at command instantiation should be done there.  Ex: can't send money to self.
#corner-cases: either wreite code for them, or be woken up (being woken up DOES NOT SCALE)
#sage: no need for command flags since serialization happens at the received-event level
#sagas, aggregates, event handlers, and queryhandlers only exist for a few milliseconds
#Raise your hand if you think we should think about and plan code before we start coding
    #Remember the result of this poll because our decision making abilities are not to be trusted when we sit down to code.
#Can't we just do it the 'normal' way? -- Usually lacks accountability for corner cases.  "I don't know why it does that sometimes!"  "No one gets that.  Just adjust it and move on" "Meh"
#saga is OPTIMISTIC concurrency, so it avoids the confusing process of locking rows and tables
#ensures system is ALWAYS in a consistent state.... eventually
#When sagaing, account for EVERY possible outcome of a command to have an exhaustive state machine
#state is only ever set via events.  Everything else is transient ie worthless.  setting state is only ever meant for validation purposes
#architecture lends itself to extreme predictability - fundamentally different since you don't think when you sit down to code
#upgrading events (add fields, but don't remove or change them)
#migrating from another architecture
#explain what happens on every interaction if the power goes out, or if the remote end disconnects or is unreachable
#List all architectural rules and explain what happens if any of them are broken!
#explain eventual consistency
#explain that different parts of this framework can be used in different processes as needed
#explain why this pattern is fast
#events are NOT deleted, a compensating event is issued.  This doesn't apply in the case of right to forget laws
#when dealing with transaction histories, include the current amount with each transaction event to avoid calculating it on the read side with partial data
#variations on pattern: Have only event handlers from another system to build views.  No 'Command' side
#handle out of order and duplicate events
#No need to write anymore SQL!
#reflection is NOT slow because you do it only once and then you cache it!
#duplicating data is fine.  Makes the queries a lot simpler
#everyone involved in a saga needs to understand that if they haven't heard anything about the transaction after a certain period, it should be timed out
    #without this, system resources can be consumed indefinitely
#if a command is suppoosed to work but doesn't, backoff.  If it can fail, then just kill the saga
#if a command is idempotent and the thing happens again, return a successfuyl response
#saga command handlers need to be idempotent, the transaction_id can be used for this
#events received to do saga stuff should be marked as handled once the saga is finished handling them in case the 
    #power dies before the saga finishes handling the event
#assume the process might die at any moment
#saga commands should be idempotent
#Learn everything the app does by looking at the commands and events
#Don't reuse events for different purposes.  Just create a new event ex: transferRejectedEvent is diff for requestor and requestee
#bake Privacy into aggregate logic.  Ex: what happens if an account is perma-deleted while a saga is in progress?
    #saga should listen for deleted event from the account and kill itself
#sagas don't output events because that would require spinning up an aggregate to verify that the event is still valid
    #sagas use command in case there's a rejection
    #each command that a saga issues must either be inconsequential, or must have a rollback command
#don't put the name of an entity into an event, use a PLK instead.  Names change and should be put into their own event stream
#command handlers NEVER update state.  Use a reconstitute state decorator for that
#figure out a state_changes dependencies and package them together into a single aggregate.  No larger than this!
#commands must always be value (use guard clauses)
#If a command needs multiple aggregates, use a saga instead
#If aggregates need to talk to each other, you've defined the domain incorrectly
#AccountIDCreation with an incrementing iD would use a saga

# def some_func(num_times):
#     def decorator(func):
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             for _ in range(num_times):
#                 value = func(*args, **kwargs)
#             return value
#         return wrapper
#     return decorator


#Propel todos
#add user identity to events
#maybe add correlation ID to command/events



#Pending explanations
    #command response
    #how not to use kafka
    #GUID name lookups
    #showing account totals on query side



#EXAMPLE QUERY
# INSERT INTO accounts(id, name, name_version, amount, amount_version) VALUES('0001', 'bob', 1, 42, 2)
# ------------------

# SELECT * FROM accounts

# ------------------

# UPDATE accounts SET 
# 	name= CASE WHEN name_version < 2 THEN 'sally' ELSE name END,
# 	name_version=	CASE WHEN name_version < 2 THEN 2 ELSE name_version END,
# 	--amount=			CASE WHEN amount_version < 3 THEN 44 ELSE amount END,
# 	--amount_version=	CASE WHEN amount_version < 3 THEN 3 ELSE amount_version END
# WHERE id = '0001'

# INSERT INTO accounts(id, name, name_version) VALUES ('0001', 'sally', 2) 
# ON CONFLICT DO UPDATE SET 
# 	name= CASE WHEN name_version < 2 THEN 'sally' ELSE name END,
# 	name_version=	CASE WHEN name_version < 2 THEN 2 ELSE name_version END
	
# INSERT INTO accounts(id, amount, amount_version) VALUES ('0001', 80, 3) 
# ON CONFLICT DO UPDATE SET 
# 	amount= CASE WHEN amount_version < 3 OR amount_version is NULL THEN 42 ELSE amount END,
# 	amount_version=	CASE WHEN amount_version < 3 OR amount_version is NULL THEN 3 ELSE amount_version END

# INSERT INTO accounts(id, name, name_version) VALUES ('0001', 'polly', 3) 
# ON CONFLICT DO UPDATE SET 
# 	name= CASE WHEN name_version < 3 THEN 'polly' ELSE name END,
# 	name_version= CASE WHEN name_version < 3 THEN 3 ELSE name_version END
	
# ------------------

# DELETE FROM accounts WHERE id = '0001'


#View Ideas:
    #Show closed accounts with credits