# pgnotifier
A simple little utility to capture and process Postgresql NOTIFY streams

#### Features
* Monitor multiple channels at once
* Register multiple callbacks to any number of channels
* Add and remove channels at will
* Add and remove callbacks at will
* Abstracts away asynchronous context for synchronous use
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

``` sql
select pg_notify('my_app_name', '"WARNING: Something really bad happened"');
select pg_notify('ch1', '{"topic": "abc", "data": "some data", "something": "else"}');
select pg_notify('ch2', '{"topic": "xyz", "notice": "update", "data": [2, "stuff"]}');
select pg_notify('ch3', '[1,2,3,4,5]');
```
Back in python, the payload is passed to callbacks subscribed to channel `my_app_name`, `ch1`, etc. The payload is cast to it's native python type via `ast.literal_eval`. See https://docs.python.org/3/library/ast.html and https://docs.python.org/3/library/ast.html#ast.literal_eval

> [!IMPORTANT]
> Postgresql notifications must be text and must be shorter than 8000 bytes. It is recommended to only send the key of a record, or a view or table name, a function reference, etc.


## API

#### ``Notifier(db_conf)``
Constructor.

Args:
 * `db_conf` database configuration, as `dict`.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
```
---


#### ``add_channels(channels, autorun=True)``
Adds one or more channels to the set of channels to monitor. Is a no-op if channel already exists. Optionally restarts listener thread.

Args:
 * `channels` list of channels to add, as `str` (single channel), `list` or `set`.
 * `autorun` restart listener thread with new channels added, as `bool`. Default is `True`.

> [!NOTE]
> Added channels *can only* be monitored by disposing and recreating the database connection and listener thread (as the notifier blocks). This mechanism happens automatically when `autorun=True`. Otherwise, if `autorun=False`, added channels *will not* be monitored until a call to `stop()` and `run()` or `restart()` is made.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
n.add_channels(['my_app_name', 'ch1', 'ch2', 'ch3'])
```
---


#### ``add_channel(channel, autorun=True)``
Alias for `add_channels(...)`, as a non-pluralised naming convenience.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
n.add_channel('ch4')
```
---


#### ``get_channels()``
Returns the set of registered channels, as `set`.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
s = n.get_channels()
print("channels: ", s)
```
---


#### ``remove_channels(channels, autorun=True)``
Removes one or more channels from the set of channels to monitor. Is a no-op if channel doesn't exist. Optionally restarts listener thread.

Args:
 * `channels` list of channels to remove, as `str` (single channel), `list` or `set`.
 * `autorun` restart listener thread with channels removed, as `bool`. Defaults to `True`.

> [!NOTE]
> Removed channels *will only* cease being monitored by disposing of, and recreating the database connection and listener thread (as the notifier blocks). This mechanism happens automatically when `autorun=True`. Otherwise, if `autorun=False`, removed channels *will* continue to be monitored until a call to `stop()` and `run()` or `restart()` is made.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.remove_channels(['my_app_name', 'ch2'])
s = n.get_channels()
print("channels: ", s)
```
---


#### ``remove_channel(channel, autorun=True)``
Alias for `remove_channels(...)`, as a non-pluralised naming convenience.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.remove_channel('ch4')
s = n.get_channels()
print("channels: ", s)
```
---


#### ``subscribe(id, channel, fn, autorun=True)``
Adds a callback function with id for notifications on channel. Creates channel if channel does not exist. Optionally restarts listener thread.

Args:
 * `id` subscriber id, as `hashable` (i.e. any immutable type such as strings, numbers, and tuples containing immutable types).
 * `channel` notification channel to subscribe to, as `str`.
 * `fn` callback function, as `callable` (i.e. function or method).
 * `autorun` restart listener thread if new channel added, as `bool`. Defaults to `True`.

When a notification is received on a channel, callbacks subscribed to that channel will be executed.

Args:
 * `id` the subscriber `id` as `hashable`.
 * `channel` the notification channel, as `str`.
 * `payload` the notification received, as native type as cast by `ast.literal_eval`.
 * `pid` the notifying sessions server process PID, as `int`.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
n.subscribe(42, 'ch1',
    lambda id, channel, payload, pid: print("id: ", id, ", channel: ", channel,
        ", payload: ", payload, ", pid: ", pid))
```
---


#### ``get_subscriptions()``
Returns channel -> subscription mappings, as `dict`.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
d = n.get_subscriptions()
print("subscriptions: ", d)

```
---



#### ``unsubscribe(id, channel, autorun=True)``
Removes a callback function with id from notifications on channel. Also removes channel if that channel no longer contains any subscriptions. Optionally restarts listener thread.

Args:
 * `id`  the subscriber id, as `hashable`.
 * `channel` notification channel to unsubscribe from, as `str`.
 * `autorun` restart listener thread if channel removed, as `bool`. Defaults to `True`.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.unsubscribe(42, 'ch1')
```
---


#### ``restart()``
(Re)starts listener thread and recreates database connection. *This function is generally not needed in userland.*

> [!NOTE]
> Only necessary under the following conditions:
> * Channels have been added or removed with arg `autorun=False`.
> * Subscribers have been added or removed with arg `autorun=False`, and in the process, have themselves created or removed channels.
> * Notifier was previously stopped by a call to `stop()`.
> * No channels and no subscribers have been added to Notifier and no call to `run()` or `restart()` has been made.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been changed with arg autorun=False ...
n.restart()
```
---


#### ``start()``
Starts the listener thread (if not already running). Is a no-op if thread already running. *This function is generally not needed in userland.*

Establishes database connection and spins off a thread to monitor notify channels and execute subscribed callbacks.

> [!NOTE]
> Only necessary under the following conditions:
> * Channels have been added or removed with arg `autorun=False`.
> * Subscribers have been added or removed with arg `autorun=False`, and in the process, have themselves created or removed channels.
> * Notifier was previously stopped by a call to `stop()`.
> * No channels and no subscribers have been added to Notifier and no call to `run()` or `restart()` has been made.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added removed, etc. ...
n.start()
```
---


#### ``stop()``
Stops the listener thread (if running) and closes the database connection. Is a no-op if thread is not running.


``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added removed, etc. ...
n.stop()
```
