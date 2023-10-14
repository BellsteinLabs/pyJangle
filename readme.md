# PyJangle
## _The hard parts of event-driven applications, done for you_

PyJangle is a framework that enables you to build event-driven applications.  It takes its inspiration from many concepts, design patterns and practices including: 

- [Sagas][sagas]
- [CQRS][cqrs]
- [Domain-Driven-Design][ddd]
- [Event Sourcing][event_sourcing]
- [Eventual Consistency][eventual_consistency]

The [pyjangle package's docstring][pyjangle] contains all the relevant bits you will need to get started.  There is a reference implementation of the framework in the example package which also has a detailed [docstring][example].  The [pyjangle_sqlite3][pyjangle_sqlite3] package shows how you would go about persisting data in your chosen technology (postgres, mongo, mysql, kafka, etc.).  The interfaces are pretty lightweight and minimal, so it shouldn't be too much effort.  The [pyjangle_json_logging][pyjangle_json_logging] package is recommended to use right off the bat--it's easier to read than the default python logging.

## Framework Primer

It's best to start this section off with a diagram showing potential interactions within the framework.  Your architecture and implementation may differ, but this is a decent reference to begin modeling from.  This primer will sometimes refer to the [reference implementation][example], so it may be useful to familiarize yourself with it in addition to the diagram.  It's recommended that you at least review the [scenario and ubiquitous language][example] before proceeding. (it's pretty short)

![Potential interactions in the PyJangle Framework](/images/pyjangle_diagram.png)


### Commands and the Command Handler

A benefit of using pyjangle is rather than sifting through the source code to figure out what actions one can take in the system, you need only look at the list of [availale commands][commands].  It's important that commands are always immutable, consistent, and valid.  While the example does not showcase this, it is common to add information to a command such as user credentials, correlations IDs, etc.  Command names should always be imperative.

A command is something the user of your system (user could also refer to a non-human like a computer or monkey) wants to do.  The line on the diagram between the command and command handler could be a user clicking a button causing a commnd to be sent to our command handler.  You might have an API endpoint of your web application mapped to the registered command dispatcher which is usually a function called [`handle_command`][handle_command].  Notice all of the lines coming in and out of the command handler--it's nice to have a framework already written that orchestrates all of that those interactions!  

Mapping a command to an aggregate is one of the first tasks of the command handler.  This mechanism is informed by the [`RegisterAggregate`][RegisterAggregate] decorator which locates all methods in the decorated aggregate that are themselves decorated with a [`validate_command`][validate_command] decorator.  Once the aggregate is identified, it is instantiated in its pristine state.  Next, the command handler looks for a snapshot (cached copy of the aggregate state) of the aggregate, if snapshots are being used; snapshots are an opt-in feature that aren't always necessary.  Next, the event store is queried for all of the aggregate's events between where the snapshot left off and the most recent event--the whole point of the snapshot is to reduce the number of events that need to be queried.  The events are passed to the aggregate's [`apply_events`][apply_events] method which then calls methods that you've decorated with the [`reconstitute_aggregate_state`][reconstitute_aggregate_state] decorator.

Now that the aggregate has been brought from its pristine state to the current *version* of reality, the command can now be validated against it.  If the command is determined to be not valid, a [`CommandResponse`][CommandResponse] with a `False` value is returned from the command handler.  Your endpoint might return a 400 Bad Request type message or whatever you deem appropriate.  If the command is validated, new events are emitted by the aggregate.  Those events are then published to the event store, and if applicable, a new snapshot is created.  

It can happen, especially in a high-traffic system on an especially busy aggregate, that two identical commands arrive at the *same time* from two different users.  It might only make sense for one of these commands to validate successfully.  In this case, whoever publishes their events to the event store first, wins.  For the other user, the command handler will internally repeat the process of instantiating a new aggregate, applying snapshots, applying events (with the new event from the other user), and attempting to validate the command.  This new state *may* cause the event to fail (depending on the aggregate's business logic) at which point, a negative `CommandResponse` is returned from the command handler.  This retry is PyJangle's implementation of optimistic concurrency, and it's a major part of what makes this framework fast and reliable.  

### Events are committed and published... now what?

After an event is published, it usually needs to be utilized by some other component, internal or external to your application--we call this *dispatching* the event.  The [Event Dispatcher][event_dispatcher], is *optional* and depends on the implementation of the [`EventRepository`][EventRepository].  Two things must happen when an event is created: the event must be committed to storage, and the event must be made available to subscribers.  [Apache Kafka][kafka] serves as both an Event Repository and a subscription service for events, so disptaching events after they've been committed to the event store would be uncecessary since Kafka does this for you.  

But let's say you're not using Kafka--let's assume you're using MySql as your event store and RabbitMQ (RMQ) as a message bus (configured to be durable).  In this case, you would register an event dispatcher using [`register_event_dispatcher`][event_dispatcher] that pushes messages to RMQ.  Whenever an event dispatcher is registered, it pulls events from the 'Event Dispatcher Queue' which is created by the framework with a max size defined by the `EVENTS_READY_FOR_DISPATCH_QUEUE_SIZE` environment variable.  The framework places committed messages onto this queue to allow the `CommandResponse` to be returned to the user without having to wait for event dispatching to complete.

In both the Kafka and RMQ cases, it becomes easy to separate out the 'command' portion of the diagram, on the left, from the event-handling/query portion on the right.  You could put them into separate processes to independently handle load on the command and query portions of your application with RMQ or Kafka as the intermediary.

Finally, let's assume you're creating a new project, and aren't interested in setting up RMQ or Kafka, or anything like that.  The out-of-the-box simple solution is to start an internal background task in the current process that constantly pulls from the 'Event Dispatcher Queue' on the diagram.  The [`init_background_tasks`][initialize] method in the [`initialize`][initialize] module defaults `process_committed_events` to `True` which does just this.  The [`default_event_dispatcher`][event_dispatcher], which is configured for you by default in [`init_background_tasks`][initialize], will map the event to a registered 'Event Handler'.  Event handlers are registered with [`register_event_handler`][event_handler].  The diagram shows four flavors of event handlers attached to the event dispatcher.  This is by no means an exhaustive list of what could be done in an event handler.  An event handler could send an SMS, send an e-mail, turn on the light in your garage... you get the idea.

>There is no guarantee from kafka, or RMQ, or pretty much anything that you might not get an event [more than once][message_delivery].  Ensure that your event handlers are *idempotent* when implementing a distributed system.

The simplest case is probably that of an event handlerupdating an application database that users will query via some mechanism such as an API, CLI, mobile application, etc to get the current state of the application.  To do this in an idempotent manner, look at the query_builder in the [`pyjangle_sqlite3`][query_builder] package for inspiration.  It boils down to using atomic conditional updates to populate application view tables.

Another case would be when you need your event handler to fire off a command such as the [TransferDebited][transfer_event_handler] event handler in the [reference project][example].  This is a narrow use case in between a simple event handler and an event handler that instantiates a saga.  This case doesn't require a full-blown saga, but you need some guarantee that if the command fails, the event will be handled again at some point [until it succeeds][event_daemon].

Another class of event handlers, as was previously mentioned, are those that instantiate a saga.  PyJanlge makes these cases relatively simple.  Sometimes, the event that's emitted is the first in a chain of events and commands known as a [`saga`][saga].  In the reference project, take a look at the [`RequestSaga`][example_saga] feature for a situation where this is warranted.  It is because the request involves coordination across multiple aggregates that a saga becomes relevant.  In this case, the event handler would simply defer to [`handle_saga`][saga_handler], and assuming that the [`Saga`][saga] has been created and registered, the framework will take care of the rest.

### Event processed, nowhat?

Regardless of the event handler, if it's successful, it will be marked as completed on the component that keeps track of such things.  This can take on many forms depending on the technology that's being used, but a simple case is that there's a table containing events that have not yet been handled.  Removing an event from that table moves it to the 'handled' state.  Technologies like RMQ and Kafka provide their own means of confirming a message.

If events are not handled appropriately, meaning the event handler throws an exception, the event is not marked handled, and will eventually be picked up by the event daemon which will redispatch events until they are marked as 'handled'.

### Sagas

You'll notice a littany of interactions coming out of the saga block on the diagram.  Once a saga requiring event is dispatched, the event handler instantiates the relevant saga. (This is all hidden away behind the [`handle_saga_event`][saga_handler] method in the framework)  All previous events corresponding to the saga are retrieved in much the same way it's done when applying events to an aggregate.  The saga will update itself with the old events as well as the newly arrived one, and based on the current state, it will decide what needs to happen next.  Commonly the response involves issuing a command which will either succeed or fail.  Regardless, the saga updates its state with the outcome, and either wakes up when another event arrives or when it is instantiated by the [saga daemon][saga_daemon].

### Saga Daemon

When a command issued by a saga fails, the failure takes the form of a Command Response which is recorded by the saga via an event.  The [saga handler][saga_handler] will also update the saga's metadata to reflect that the command failed and the saga should be retried at a later time.  Retrying at a later time is accomplished by the [saga daemon][saga_daemon] which queries the sagas' metadata from the [`SagaRepository`][SagaRepository], and retries all sagas that require it.  When the sagas requiring a retry because of a failed command retry their commands, the will either succeed and progress their state, or fail and update their metadata to reflect that a retry is required.

### Queries

So far, we've written data and changed the application state, but that sort of thing is generally done in response to something a user sees on a screen or other interface.  For example, if I see that my bank balance is low in the mobile app, I might deposit a check from a friend to increase the balance.  My query is mapped to a query handler registered with [`register_query_handler`][query_handler], and it's up to the query handler function to fetch the data from some kind of application data store and return it back to the user.  It's pretty straightforward compared to everything else we've done so far.

### Logical vs Physical Separation

In the diagram, you'll notice that the Event Store, Snapshots Store, Saga Store, and Application Views are all in separate boxes.  Your app doesn't need to use 4 separate databases--there's nothing wrong with putting everything into the same database, but in separate tables, if that's what's appropriate for you.  If you really want speedy queries from a specific technology that's managed independently, go for it!  There's a lot of flexibility to be had here, so do what makes sense.

>It's worth noting that the *only* components that query the event store are the Command Handler and Event Daemon.  It is an error for any other component to query the event store directly.  Events should be 'denormalized' or processed to a format that is consumable by the application and put in a store that is dedicated to serving queries.  This is the [command-query segregation][cqrs] part of the framework.

### Eventual Consistency

It's natural to look at all the lines on the diagram and to then wonder how "realtime" this could all be.  In practice, it effectively realtime so long as you make reasonable accomadations such as connection pooling.  It takes a few milliseconds for the command to be propagated to the 'Application Views'.  There is, however, a delay.  It's conceivable that a user can take an action, and upon quickly reloading a page, not see the action reflected.  There are several approaches for mitigating this.

First, the client could establish a connection to the server via a websocket or server-sent-event and just wait for the resulting event to come through and process it directly on the client.  Another approach is to assume that once a response is receive from the command, that the 'Application Views' will eventually be updated, but if I deposit 10 dollars, I don't need a query to know that I should increment my balance by $10, right?  I could just go ahead and do it without the query.

Another approach is to hash the state of a thing that you're about to change and send the hash along with the command. (An [etag][etag] is an established and natural way of doing this.  This problem has existed since the web was invented and is not unique to this framework and its patterns.)  The aggregate would then verify the hash as a part of validating the command.  If it looks like the update was based on old data (the users screen wasn't updated), the command would fail and notify the client that the data needs to be refreshed.

It's really up to you how to handle eventual consistency.  The alternative approach is to do everything synchronously, but that approach scales *very* poorly in the general sense.  The idea of using a queue and using computing resources at the capacity they were designed to handle (by pulling from the work queue when they're ready to) rather than everything all at once will have you scaling well until the end of time!

### Event Replay

It might already be obvious to you that one of the key benefits of this framework is that the only really important data in the application is the event store and saga store.  In fact, you really only need to back up the event store and saga store which makes things pretty simple.  If this sounds strange, let's do a thought experiment (you can also do this using the interactive example in [example][example]).  Let's assume your bank application has been running for a while and someone accidentally deletes everything from the database.  The recovery process would be as follows:

1. Create a new database with the appropriate tables and indexes, etc. (The assumption is that you have your schema lying around in source control)  It's blank at this point with no data.
2. Restore your event store and saga store tables from backup.
3. Set all of your events to 'not handled' whatever that looks like for your persistence mechanism.  In other words, just run all the events through the dispatcher again.
4. You're done!

Sit back and wait a few minutes, and all of your events will repopulate all of your 'Application Views', and you're off to the races, once again.  At this point, your aggregates are working fine since they only rely on the event store and not the application views.  One thing worth elaborating on here is the importance of idempotency.

### Idempotency

Replaying events is the extreme example of getting the same data more than once, but as was stated earlier, there's generally not a guarantee that you won't get the same event more than once just because that's an inherent issue with a distributed system.  Let's assume you're doing an event replay, but you forgot to backup the saga store.  That means the sagas will reissue ALL of those commands all over again which seems like it would be a problem unless you wrote the aggregates in such a way that they are idempotent.  So if I issue a command to deposit $10, my aggregate could simply note the transaction ID, and if it sees the same transaction ID later, it should know to respond with an "OK, got it!" (not a failure).  By doing so, the saga will complete itself at the end of its workflow, and everything will be consistent.  In the absence of this, account balances would be wrong... not ideal.

### Out of Order Events

Because of the asynchronous nature of a distributed system, it's also a good idea to handle the case where events arrive out of order.  A good example is the case where my 'Application Views' get the notification that an account was deleted before it was created.  That seems odd, but it could theoritically happen, and will definitely happen with a sufficiently large and performant event replay.  The general idea is that if the deleted event is received first, create the record for the account and set the 'is_deleted' flag to True.  One the other events come in, just fill in the gaps in data.  Eventually, everything will be updated.  

In the case of gaps in a transaction ledger, it's best to not return *ANY* results in response to a query unless all entries up to a point in the transaction log are accounted for, otherwise the final balance will be incorrect.  Sometimes it's useful to include the balance in any events that modify the balance which saves the need to calculate it from piecing together a transaction log.

Let's say that hypothetically, my account has three 'NameChanged' events in its history, but they all come in out of order.  My event handlers would need to issue an atomic upsert that verifies that the event it's currently handling has a higher sequence/version number than the data that's currently in the database.  The implication here is that a separate column is maintained containing the version number of the last updater.  To facilitate this sort of query, which can be tricky to write in certain technologies, see the [`Sqlite3QueryBuilder`][query_builder] in the `pyjangle_sqlite3` package for an example of a builder that eliminates the need to write such an error-prone query (more than once).

### The Triple Shuffle

With a little practice, you'll be writing code that is resiliant to out-of-order duplicates in no time.  Again, this is a necessary evil in an asynchronous system, and all the really performant ones tend to be asynchronous, so it's something to embrace!  Instead of relying on your own skills, it's prudent to have automated tests that verify that your system has this capability:

1. Code a simulation that does everything your system is capable of (issue all of the commands).  Don't just do it once, make it a thorough simulation that creates a copious amount of events in your event store.
2. Measure the contents of each table in your 'Application Views'.  
3. Clear the application views.
4. Run an event replay, but shuffle all of the events into random order, and process them through the event handlers three times. (You can do this as many times as you'd like, but the 'Triple Shuffle' has a nice ring to it.)  
5. Repeat step 2 and compare your results to the previous measurement.

[sagas]:                        <https://learn.microsoft.com/en-us/azure/architecture/reference-architectures/saga/saga>
[ddd]:                          <https://en.wikipedia.org/wiki/Domain-driven_design>
[event_sourcing]:               <https://martinfowler.com/eaaDev/EventSourcing.html>
[eventual_consistency]:         <https://en.wikipedia.org/wiki/Eventual_consistency>
[kafka]:                        <https://kafka.apache.org/>
[message_delivery]:             <https://docs.confluent.io/kafka/design/delivery-semantics.html>
[etag]:                         <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag>
[cqrs]:                         <https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs>
[transfer_event_handler]:       <https://github.com/BellsteinLabs/pyJangle/tree/main/example/event_handlers.py>
[example_saga]:                 <https://github.com/BellsteinLabs/pyJangle/tree/main/example/saga.py>
[example]:                      <https://github.com/BellsteinLabs/pyJangle/tree/main/example/__init__.py>
[commands]:                     <https://github.com/BellsteinLabs/pyJangle/tree/main/example/commands.py>
[event_daemon]:                 <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/event/event_daemon.py>
[saga_handler]:                 <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/saga/saga_handler.py>
[saga_daemon]:                  <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/saga/saga_daemon.py>
[SagaRepository]:               <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/saga/saga_repository.py>
[query_handler]:                <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/query/query_handler.py>
[pyjangle_sqlite3]:             <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle_sqlite3/__init__.py>
[pyjangle_json_logging]:        <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle_json_logging/__init__.py>
[query_builder]:                <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle_sqlite3/event_handler_query_builder.py>
[saga]:                         <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/saga/saga.py>
[pyjangle]:                     <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/__init__.py>
[event_handler]:                <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/event/event_handler.py>
[initialize]:                   <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/initialize.py>
[CommandResponse]:              <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/command/command_response.py>
[RegisterAggregate]:            <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/aggregate/register_aggregate.py>
[validate_command]:             <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/aggregate/aggregate.py>
[apply_events]:                 <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/aggregate/aggregate.py>
[reconstitute_aggregate_state]: <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/aggregate/aggregate.py>
[handle_command]:               <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/command/command_handler.py>
[register_event_dispatcher]:    <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/event/event_dispatcher.py>
[event_dispatcher]:             <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/event/event_dispatcher.py>
[EventRepository]:              <https://github.com/BellsteinLabs/pyJangle/tree/main/src/pyjangle/event/event_repository.py>