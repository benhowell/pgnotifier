# pgnotifier
A simple little utility to capture and process Postgresql NOTIFY streams

#### Features
* Monitor multiple channels at once
* Register multiple callbacks to any number of channels
* Add and remove channels at will
* Add and remove callbacks at will
* Abstracts away all asynchronous context to allow for easy synchronous use
* Automatic type conversion of all valid python types via `ast.literal_eval`
* Persistent, immutable internals
* Tight footprint



## Install
``` python
pip install pgnotifier
```


## Usage

``` python
import sys
from pgnotifier import Notifier

print("Ctrl-C to exit", file=sys.stderr)

conf = {
    'dbname': "my_db_name",
    'user': "my_db_user",
    'password': "my_password",
    'host': "my_host",
    'port': "my_port",
}
n = Notifier(conf)
n.add_channels(['my_app_name', 'ch3'])


n.subscribe(42, 'ch1',
    lambda id, channel, payload, pid: print(
        "callback id: ", id, ", channel: ", channel,
        ", payload: ", payload, ", pid: ", pid))


n.subscribe('an_id', 'ch2',
            lambda *_: print("I'm just going to ignore that."))

def do_complex_thing(id, channel, payload, pid):
    for k,v in payload.items():
        print("doing something with: ",v)
        # do something else
        # I think you get the idea...

# subscriber with tuple id
n.subscribe((2, 'another_id'), 'ch2', do_complex_thing)

```


## Test
From the Postrgesql end, send TEXT or JSON string notifications like so:

``` plsql
select pg_notify('my_app_name', '"WARNING: Something really bad happened"');
select pg_notify('ch1', '{"topic": "abc", "data": "some data", "something": "else"}');
select pg_notify('ch2', '{"topic": "xyz", "notice": "update", "data": [2, "stuff"]}');
select pg_notify('ch3', '[1,2,3,4,5]');
```
Back in python, the payload is passed to callbacks subscribed to channel `my_app_name`, `ch1`, etc. The payload is cast to it's native python type via `ast.literal_eval`. See https://docs.python.org/3/library/ast.html and https://docs.python.org/3/library/ast.html#ast.literal_eval

[!NOTE] Postgresql notifications must be text and must be shorter than 8000 bytes. It is recommended to only send the key of a record, or a view or table name, a function reference, etc.

[!NOTE]
> Useful information that users should know, even when skimming content.

> [!TIP]
> Helpful advice for doing things better or more easily.

> [!IMPORTANT]
> Key information users need to know to achieve their goal.

> [!WARNING]
> Urgent info that needs immediate user attention to avoid problems.

> [!CAUTION]
> Advises about risks or negative outcomes of certain actions.




## API

#### ``Notifier(db_conf)``
Constructor.

Args:
 * `db_conf` database configuration, as `dict`


``` python
n = Notifier(conf)
```
---


#### ``get_channels()``
Returns the set of registered channels, as `set`


``` python
s = n.get_channels()
```
---


#### ``add_channels(channels, autorun=True)``
Adds one or more channels to the set of channels to monitor. Is a no-op if channel already exists.
Optionally restarts listener.


Args:
 * `channels` list of channels to add, as `str` (single channel), `list` or `set`
 * `autorun` restart listener with new channels added, as `bool`. Default is `True`.

[!NOTE] Added channels *can only* be listened to by disposing and recreating the database connection and listener loop (as the notifier blocks). This mechanism happens automatically when `autorun=True`. Otherwise, if `autorun=False`, added channels *will not* be listened to until a call to `stop()` and `run()` or `restart()` is made.

__NOTE:__ Added channels *can only* be listened to by disposing and recreating the database connection and listener loop (as the notifier blocks). This mechanism happens automatically when `autorun=True`. Otherwise, if `autorun=False`, added channels *will not* be listened to until a call to `stop()` and `run()` or `restart()` is made.


``` python
n.add_channels(['my_app_name', 'ch1', 'ch2', 'ch3'])
```
---


#### ``add_channel(channel, autorun=True)``
Alias for `add_channels(...)`, as a non-pluralised naming convenience.


``` python
n.add_channel('ch4')
```
---


#### ``remove_channels(channels, autorun=True)``
Removes one or more channels from the set of channels to monitor. Is a no-op if channel doesn't exist. Optionally restarts listener.

Args:
 * `channels` list of channels to remove from the channel list, as `str` (single channel), `list` or `set`
 * `autorun` restart listener with channels removed, as `bool`. Defaults to `True`.

[!NOTE] Removed channels *will only* cease being monitored by disposing and recreating the database connection and listener loop (as the notifier blocks). This mechanism happens automatically when `autorun=True`. Otherwise, if `autorun=False`, removed channels *will* continue to be listened to until a call to `stop()` and `run()` or `restart()` is made.

__NOTE:__ Removed channels *will only* cease being monitored by disposing and recreating the database connection and listener loop (as the notifier blocks). This mechanism happens automatically when `autorun=True`. Otherwise, if `autorun=False`, removed channels *will* continue to be listened to until a call to `stop()` and `run()` or `restart()` is made.


``` python
n.remove_channels(['my_app_name', 'ch2'])
```
---


#### ``remove_channel(channel, autorun=True)``
Alias for `remove_channels(...)`, as a non-pluralised naming convenience.


``` python
n.remove_channel('ch4')
```
---


#### ``get_subscriptions()``
Returns channel -> subscription mappings, as `dict`.


``` python
s = n.get_subscriptions()
```
---


#### ``subscribe(id, channel, fn, autorun=True)``
Adds a callback function with id for notifications on channel. Creates channel if channel does not exist. Optionally restarts listener.

Args:
 * `id` subscriber id, as `hashable` (i.e. any immutable type such as strings, numbers, and tuples containing immutable types)
 * `channel` notification channel to subscribe to, as `str`
 * `fn` callback function, as `callable` (i.e. function or method).
 * `autorun` restart listener if new channel added, as `bool`. Defaults to `True`.

When a notification is received on a channel, subscribers to that channel will have their callbacks executed.

Args:
 * `id` the subscriber `id` as `hashable`
 * `channel` the notification channel, as `str`
 * `payload` the notification received, as native python type as cast by `ast.literal_eval`
 * `pid` the notifying sessions server process PID, as `int`


``` python
n.subscribe(42, 'ch1',
    lambda id, channel, payload, pid: print("id: ", id, ", channel: ", channel,
        ", payload: ", payload, ", pid: ", pid))
```
---


#### ``unsubscribe(id, channel, autorun=True)``
Removes a callback function with id from notifications on channel. Also removes channel if that channel no longer contains any subscriptions. Optionally restarts listener.

Args:
 * `id`  the subscriber id, as `hashable`
 * `channel` notification channel to unsubscribe from, as `str`
 * `autorun` restart listener if channel removed, as `bool`. Defaults to `True`.


``` python
n.unsubscribe(42, 'ch1')
```
---


#### ``restart()``
(Re)starts notify listener and recreates database connection. *This function is generally not needed in userland.*

__NOTE:__ Only necessary under the following conditions:
 * Channels have been added or removed with arg `autorun=False`
 * Subscribers have been added or removed with arg `autorun=False`, and in the process, have themselves created or removed channels.
 * Notifier was previously stopped by a call to `stop()`
 * No channels and no subscribers have been added to Notifier and no call to `run()` or `restart()` has been made.

``` python
n.restart()
```
---


#### ``start()``
Starts the Notifier (if not already running). Is a no-op if thread already running. *This function is generally not needed in userland.*

Establishes database connection and spins off a thread to listen to notify channels and execute subscribed callbacks.

__NOTE:__ Only necessary under the following conditions:
 * Channels have been added or removed with arg `autorun=False`
 * Subscribers have been added or removed with arg `autorun=False`, and in the process, have themselves created or removed channels.
 * Notifier was previously stopped by a call to `stop()`
 * No channels and no subscribers have been added to Notifier and no call to `run()` or `restart()` has been made.

``` python
n.start()
```
---


#### ``stop()``
Stops the Notifier.

Cancels the listen task and closes the database connection.

``` python
n.stop()
```
