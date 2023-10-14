"""A reference implementation of pyjangle classes.

The example scenario defined in this project is an implementation of a banking scenario.
To run the example, see the `driver_example` and `main` modules below.

Ubiquitous Language:
    Account:
        - Can be created at runtime.  
        - Has a name, account ID, and a balance.  
        - Handles deposits, withdrawals, transfer funds to another account, or request 
          funds from another account.
    Deposit:
        - The act of adding funds to an account.
    Withdrawal:
        - Removing funds from an account.
    Transfer:
        - When an account sends funds to another account.
    Request:
        - When account 'A' requests funds from account 'B'.  Account 'B' can either 
          approve or deny the request. 
    Debt Forgiveness:
        - Twice, in the lifetime of an account, a debt up to $100 can be forgiven which
          resets the account balance to zero.   
    Debit:
        - When funds are removed from an account.
    Credit:
        - When funds are added to an account.
    Rollback:
        - If a transfer or request fails, undoing any funds transfers that have occurred
          is called a rollback.

Module Overview:
    driver_example:
        A choreographed implementation of the banking scenario.
    main:
        An interactive implementation of the banking scenario.
    commands:
        All possible actions that can be taken such as withdrawls, deposits, transfers,
        etc.
    events:
        All possible state changes that can occur to the domain such as funds withdrawn,
        funds deposited, transfer_debited, etc.
    queries:
        The various 'reads' that can be accomplished against the application domain such 
        as retrieving an accounts list, a ledger for a specific account, or a detailed 
        summary of a single account.
    query_responses:
        DTOs for query responses.
    saga:
        Distributed transactions.  In this case, a 'Request' (see the ubiquitous 
        language) is the only saga.
    account_aggregate:
        Processes commands related to an account and emits the relevant events.  
        Concisely and neatly encapsulates all business logic for an account.
    account_creation_aggregate:
        The aggregate that contains the state required to create an account.  Account 
        IDs are created in numerical order, so it can be said that the next assignable 
        ID is a part of the application state, and it does not make sense to put this 
        state into a specific account aggregate necessitating the need for a 
        'creational'-type aggregate.
    data_access.bank_data_access_object:
        The interface used to both respond to queries and update application-specific
        data 'views' in repsonse to new events.
    data_access.sqlite3_bank_data_access_object:
        Sqlite3 concrete implementation of bank_data_access_object.
    validation.descriptors:
        Python descriptors for query, command, and event fields to facilitate 
        immutability.

Config:
    Environment Variables:
        DB_JANGLE_BANKING_PATH
            Configures the file location of the application-specific database.  It is 
            optional whether this is the same database as JANGLE_EVENTS_PATH, 
            JANGLE_SNAPSHOTS_PATH, and/or JANGLE_SAGAS_PATH.
"""

