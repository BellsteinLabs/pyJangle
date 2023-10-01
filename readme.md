# PyJangle
## _The hard parts of event-driven applications, done for you_

PyJangle is a framework that enables you to build event-driven applications.  It takes its inspiration from many concepts, design patterns and practices including: 

- [Sagas]()
- [CQRS]()
- [Domain-Driven-Design]()
- [Event Sourcing]()
- [Eventual Consistency]()

The [pyjangle package's docstring]() contains all the relevant bits you will need to get started.  There is a reference implementation of the framework in the pyjangle_example package which also has a detailed [docstring].  The [pyjangle_sqlite3] package shows how you would go about persisting data in your chosen technology (postgres, mongo, mysql, kafka, etc.).  The interfaces are pretty lightweight and minimal, so it shouldn't be too much effort.  The [pyjangle_json_logging] package is recommended to use right off the bat--it's easier to read than the default python logging.

## Framework Primer

It's best to start this sectino off with a picture that shows every interaction within the framework.  Your architecture and implementation may differ, but this is a decent reference implementation to work from.  The primer will sometimes refer to the [reference implementation](), so it's not a horrible thing to be familiar with.  It's recommended that you at least review the [scenario and ubiquitous language]() before proceeding. (it's pretty short)


### Commands and the Command Handler

A benefit of using pyjangle is rather than sifting through the source code to figure out what actions one can take in the system, you need only look at the list of [availale commands]().  It's important that commands are always immutable, consistent, and valid.  It's common to add information to a command such as user credentials, correlations IDs, etc.  Command names should always be imperative.

A command is something the user of your system (user could also refer to a non-human like a computer or monkey) wants to do.  In our diagram, line 1A shows the user clicking a button and a commnd is send to our command handler.  You might have an API endpoint of your web application mapped to the registered command dispatcher which is usually a function called `handle_command`.  Notice all of the lines coming in and out of the command handler, and it should be clear why it's nice to have a framework already written that orchestrates all of that spaghetti.  

Line 2A is where the command is mapped to an aggregate by way of the `RegisterAggregate` decorator.  That decorator causes the framework to locate all methods in the aggregate decorated with a `validate_command` decorator.  Once the aggregate is identified, it's instantiated in its pristine state.  Next, the command handler looks for a snapshot (cached copy of the aggregate state) of the aggregate if snapshots are being used; snapshots are an opt-in feature that aren't always necessary.  The snapshot is returned in line 4B.  Next, the event store is queried for all of the aggregate's events between where the snapshot left off and the most recent event--the whole point of the snapshot is to reduce the number of events that need to be queried.  In line 4A, the aggregate is updated with the events to build it's current state.  You write the code for this part using the `reconstitute_aggregate_state` decorator.

The aggregate can now validate the command from line 1A.  If the command is determined to be not valid, a `CommandResponse` with a False value is returned from the command handler.  Your endpoint might return a 400 Bad Request type message or whatever you deem appropriate.  If the command is validated, new events are emitted by the aggregate.  Those events are then published to the event store in line 5A, and if applicable, a new snapshot is created on line 5B.  

It can happen, especially in a high-traffic system on an especially busy aggregate, that two identical commands come in at the same time from two different users.  It might only make sense for one of these commands to validate.  In this case, whoever publishes their events to the event store on line 5A first, wins.  For the other user, the command handler will internally repeat the previous steps starting from 3A, and this time, they'll get a new event on line 4A of the diagram reflecting the other users action.  This new state may cause the event to fail at which point, a negative `CommandResponse` is returned from the command handler.  This feature is an implementation of optimistic concurrency, and it's a major part of what makes this framework fast and reliable.

Like commands, the events that are emitted from the aggregate on line 5A are immtable, consistent

### Events are committed and published... now what?

Line 6 is interesting and optional.  It can be the case that the `EventRepository` and the conceptual stream of committed messages are one-in-the-same.  If they are the same, line 6 goes away, and next up is line 8 is effectively a background task, parallel thread, or a completely separate process with a subscription to a [kafka]() stream that is receiving events and dumping them onto the function registered with `register_event_dispatcher` which corresponds to the 'Event Dispatcher' box in the diagram.  Kafka is the prototypical example of the case where the event-store and event stream are one-in-the-same, and that approach is highly recommended for this framework.  

But let's say you're not using Kafka--let's assume you're using MySql as your event store and RabbitMQ (RMQ) as a message bus (configured to be durable).  In this case, you would register an event dispatcher using `register_event_dispatcher` that pushes messages to RMQ.  Whenever an event dispatcher is registered, the 'Event Dispatcher Queue' is created by the framework with a max size defined by the `EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE` environment variable.  This is just to relieve pressure in the general case where event handlers are running slowly for whatever reason.  

In both the Kafka and RMQ cases, it becomes easy to separate out the 'command' portion of the diagram, on the left, from the event-handling/query portion on the right.  You could put them into separate processes to independently handle load with RMQ or Kafka as the intermediary.

Finally, let's assume you're creating a new project, and aren't interested in setting up RMQ or Kafka, or anything like that.  The out-of-the-box simple solution is to start an internal background task in the same process that constantly pulls from the 'Event Dispatcher Queue' on the diagram.  The `init_background_tasks` method in the `initialize` module defaults `process_committed_events` to `True` which does just this.  The `default_event_dispatcher`, which is configured for you by default in `init_background_tasks`, will map the event to a registered 'Event Handler'.  Event handlers are registered with `register_event_handler`.  The diagram shows three event handlers on the receiving ends of lines 9A, 9B, and 9C.  This is by no means an exhaustive list of what could be done in an event handler.  It might send an SMS, an e-mail, turn on the light in your garage... you get the idea.

    There is no guarantee from kafka, or RMQ, or pretty much anything that you might not get an event [more than once]().  When authoring 

The simplest case is 9C.  The event has some data that is used to update the database that our users query via some mechanism such as an API, CLI, mobile application, etc.  

Sometime, you need your event handler to fire off a command such as the [Transfer]()example in the reference project.  This is represented by line 9B in the diagram.  This fits a narrow case in between 9C and 9A.  It doesn't require a full-blown saga, but you need some guarantee that if the command fails, the event will be handled again at some point [until it succeeds]().

9A is probably the most involved case, and made much simpler by using PyJangle.  Sometimes, the event that's emitted is the first in a chain of events and commands known as a [saga].  In the reference project, take a look at the [request]() feature for a situation where this is warranted.  It is because the [request]() involves coordination across multiple aggregates that a saga becomes relevant.  In this case, the event handler would simple defer to `handle_saga`, and assuming the `Saga` has been created and registered, the framework will take care of the rest.

### Event processed, nowhat?

Regardless of the event handler, if it's successful, line 17 on the diagram shows the event being marked as 'handled' on the event store.  This can take on many forms depending on the technology that's being used, but a simple case is that there's a table containing events that have not yet been handled.  Removing an event from that table moves it to the 'handled' state.

If events are not handled appropriately, meaning the event handler throws an exception, the event is not marked handled, and will eventually be picked up by the event daemon on line 2C of the diagram.  (Line 1C is the daemon querying for not handled events that have been published for a while)  These events are then redispatched by the daemon until they are marked as 'handled'.

### Sagas

You'll notice a littany of interactions coming out of the saga block on the diagram.  Once the event is received on line 9A, the event handler instantiates a saga on line 10A. (This is all hidden away behind the `handle_saga_event` method in the framework)  On line 11, all previous events corresponding to the saga are retrieved in much the same way it's done line 4A for aggregates.  The saga will update itself with the old events as well as the newly arrived one on line 12, and based on the current state, it will decide what needs to happen next.  Commonly the response involves issuing a command, line 13.  The command may succeed or it may fail, and that's represented on line 15.  Regardless, the saga updates its state with the outcome, line 16, and either wakes up when another event arrives, line 9A, or if it is awoken by the saga daemon.

### Saga Daemon

When a command issued by a saga fails on line 13, the failure is sent back on line 15 and recorded on line 16.  Line 16 will also update the saga's metadata to reflect that the command failed and the saga should be retried at a later time.  This is accomplished by the saga daemon which queries the sagas' metadata on line 1D, and retries all sagas that it retrieves back on line 2D.  These sagas do the same thing that happens on lines 13 and 15, but here, we label them 3E and 3D.  They issue the command, get the response, and go back to sleep either to wait on another event, or to be retried by the saga daemon at a later time.

### Queries

So far, we've written data and changed the application state, but that sort of thing is generally done in response to something a user sees on a screen or other interface.  For example, if I see that my bank balance is low in the mobile app, I might deposit a check from a friend to increase the balance.  The act of querying my account balance here is represented by line 1B.  The query is mapped to a query handler registered with `register_query_handler`, and it's up to the query handler function to fetch the data from some kind of data store, that's line 2B and the response is line 3C.  The data is then returned to the user on line 4C, it's pretty straightforward compared to everything else we've done so far.

### Logical vs Physical Separation

In the diagram, you'll notice that the Event Store, Snapshots Store, Saga Store, and Application Views are all in separate boxes.  Your app doesn't need to use 4 separate databases--there's nothing wrong with putting everything into the same database, but in separate tables, if that's what's appropriate for you.  If you really want speedy queries from a specific technology that's managed independently, go for it!  There's a lot of flexibility to be had here, so do what makes sense.

    It's worth noting that the *only* components that query the event store are the Command Handler and Event Daemon.  It is an error for line 2B, for example, to hypothetically query the event store directly.  Events should be 'denormalized' or processed to a format that is consumable by the application and put in a store that is dedicated to serving queries.  This is the [command-query segregation]() part of the framework.

### Eventual Consistency

It's natural to look at all the lines between 1A and 1B and wonder how realtime this could all be.  In practice, it effectively realtime.  It takes a few milliseconds for the command to be propagated to the 'Application Views'.  There is, however, a delay.  It's conceivable that a user can take an action, and upon quickly reloading a page, not see the action reflected.  There are several approaches for mitigating this.

First, the client could establish a connection to the server via a websocket or server-sent-event and just wait for the resulting event to come through and process it directly on the client.  Another approach is to assume that once a response is receive from the command, that the 'Application Views' will eventually be updated, but if I deposit 10 dollars, I don't need a query to know that I should increment my balance by $10, right?  I could just go ahead and do it without the query.

Another approach is to hash the state of a thing that you're about to change and send the hash along with the command. (An [etag]() is an established and natural way of doing this.  This problem has existed since the web was invented and is not unique to this framework and its patterns.)  The aggregate would verify the hash as a part of validating the command.  If it looks like the update was based on old data (the users screen wasn't updated), the command would fail and notify the client that the data needs to be refreshed.

It's really up to you how to handle it.  The alternative approach is to do everything synchronously, but that approach scales *very* poorly in the general sense.  The idea of using a queue and using computing resources at the capacity they were designed to handle (by pulling from the work queue when they're ready to) rather than everything all at once will have you scaling well until the end of time!

### Event Replay

It might already be obvious to you that one of the key benefits of this framework is that the only really important data in the application is the event store and saga store.  In fact, you really only need to back up the event store and saga store which makes things pretty simple.  If this sounds strange, let's do a thought experiment (you can also do this using the interactive example in [pyjangle_example]()).  Let's assume your bank application has been running for a while and someone accidentally deletes everything from the database.  The recovery process would be as follows:

1. Create a new database with the appropriate tables and indexes, etc.  It's blank at this point with no data.
2. Restore your event store table from backup.
3. Set all of your events to 'not handled' whatever that looks like for your persistence mechanism.  In other words, just run all the events through the dispatcher again.
4. You're done!

Sit back and wait a few minutes, and all of your events will repopulate all of your 'Application Views', and you're off to the races, once again.  At this point, your aggregates are working fine since they only rely on the event store and not the application views.  One thing worth elaborating on here is the importance of idempotency.

### Idempotency

Replaying events is the extreme example of getting the same data more than once, but as was stated earlier, there's generally not a guarantee that you won't get the same event more than once just because of the way networks work.  Let's assume you're doing an event replay, but you forgot to backup the saga store.  That means the sagas will reissue ALL of those commands all over again which seems like it would be a problem unless you wrote the aggregates in such a way that they're idempotent.  So if I issue a command to deposit $10, my aggregate could simply note the transaction ID, and if it sees it again, it should know to respond with an "OK, got it!" (not a failure).  By doing so, the saga will complete itself at the end of its workflow, and everything will be good to go.  In the absence of this, account balances would be wrong... not ideal.

### Out of Order Events

Because of the asynchronous nature of a distributed system, it's also a good idea to handle the case where events arrive out of order.  A good example is the case where my 'Application Views' get the notification that an account was deleted before it was created.  That seems odd, but it could theoritically happen, and will definitely happen with a sufficiently large and performant event replay.  The general idea is that if the deleted event is received first, create the record for the account and set the 'is_deleted' flag to True.  One the other events come in, just fill in the gaps in data.  Eventually, everything will be updated.  

In the case of gaps in a transaction ledger, it's best to not return *ANY* results unless all entries up to a point in the transaction log are accounted for, otherwise the final balance will be incorrect.  Sometimes it's useful to include the balance in any events that modify the balance which saves the need to calculate it from a transaction log.  

Let's say that hypothetically, my account has three 'NameChanged' events in its history, but they all come in out of order.  My event handlers would need to issue an atomic upsert that verifies that the event it's currently handling has a higher sequence/version number than the data that's currently in the database.  The implication here is that a separate column is maintained containing the version number of the last updater.  To facilitate this sort of query, which can be tricky to write in certain technologies, see the `Sqlite3QueryBuilder` in the `pyjangle_sqlite3` package for an example of a builder that eliminates the need to write such an error-prone query.

### The Triple Shuffle

With a little practice, you'll be writing code that is resiliant to out-of-order duplicates in no time.  Again, this is a necessary evil in an asynchronous system, and all the really performant ones tend to be asynchronous, so it's something to embrace.  Instead of relying on your own skills, it's prudent to have automated tests that verify that your system has this capability:

1. Code a simulation that does everything your system is capable of (issue all of the commands).  Don't just do it once, make it a thorough simulation that creates a copious amount of events.  
2. Measure the contents of each table in your 'Application Views'.  
3. Clear the application views.
4. Run an event replay, but shuffle all of the events into random order, and process them through the event handlers three times. (You can do this as many times as you'd like, but the 'Triple Shuffle' has a nice ring to it.)  
5. Repeat step 2 and compare your results to the previous measurement.