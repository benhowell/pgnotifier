# pgnotifier
A simple little library to capture, process, and dispatch Postgresql NOTIFY streams
<br>
### Features
* Monitor multiple channels
* Register one or more callbacks with multiple channels
* Register one or more channels with a callback
* Control callbacks on a per channel basis
* Add and remove channels at any time
* Mute and un-mute channels
* Add and remove subscribers at any time
* Mute and un-mute subscribers
* Abstract away asynchronous context for synchronous use
* Automatic str -> type conversion of all valid python types via `ast.literal_eval`
* Persistent, immutable internal data structures
* Minimalist API
* Tight footprint

<br>

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
n.add_channels(['ch1', 'ch3'])

n.subscribe(42, 'ch1',
    lambda id, channel, payload, pid: print(
        "callback id: ", id, ", channel: ", channel,
        ", payload: ", payload, ", pid: ", pid))

n.subscribe('an_id', 'ch2',
            lambda *_: print("I'm just going to ignore that."))

def do_complex_thing(id, channel, payload, pid):
    if isinstance(payload, dict) or type(payload) == dict:
        for k,v in payload.items():
            print("doing something with key:",k, "-> val",v)
            # do something else
            # I think you get the idea...
    else:
        print("payload of type: ",
              type(payload), "is not what I was expecting!")

# subscriber with tuple id
n.subscribe((2, 'another_id'), 'ch2', do_complex_thing)

```


## Test
From the Postrgesql end, send TEXT or JSON string notifications like so:

``` sql
select pg_notify('ch1', '"WARNING: Something really bad happened"');
select pg_notify('ch1', '{"topic": "abc", "data": "some data", "something": "else"}');
select pg_notify('ch2', '{"topic": "xyz", "notice": "update", "data": [2, "stuff"]}');
select pg_notify('ch2', '[1,2,3,4,5]');
select pg_notify('ch3', '[1,2,3,4,5]');
```
Back in python, the payload is passed to callbacks subscribed to channels `ch1`, `ch2`, etc. The payload is cast to it's native python type via `ast.literal_eval`. See https://docs.python.org/3/library/ast.html and https://docs.python.org/3/library/ast.html#ast.literal_eval

<br>

> [!IMPORTANT]
> Postgresql notifications must be text and must be shorter than 8000 bytes. It is recommended to only send the key of a record, or a view or table name, a function reference, etc.

<br>

## API
The methods below provide everything needed to work with pgnotifier.

**[Notifier](#notifier-dbconf-)**
- [add_channels](#notifieradd_channels-channels-)
- [remove_channels](#notifierremove_channels-channels-autoruntrue-)
- [channels](#notifierchannels-)
- [subscribe](#notifiersubscribe-id-channel-fn-autoruntrue-)
- [unsubscribe](#notifierunsubscribe-id-channel-autoruntrue-)
- [subscribers](#notifiersubscribers-)
- [mute_channels](#notifiermute_channels-channelspyrsistentpvector-)
- [unmute_channels](#notifierunmute_channels-channelspyrsistentpvector-)
- [mute_subscriber](#notifiermute_subscriber-id-channelspyrsistentpvector-)
- [unmute_subscriber](#notifierunmute_subscriber-id-channelspyrsistentpvector-)
- [start](#notifierstart-)
- [stop](#notifierstop-)
- [restart](#notifierrestart-)
- [is_running](#notifieris_running-)

<br>

## Internal helper functions
The functions below are not required outside the internals of pgnotifier. They
are publicly exposed and included here as a matter of interest.

**[Internal helper functions](#Internal-helper-functions)**
- [assoc_in](#notifyassoc_in-m-pv-v-)
- [dissoc_in](#notifydissoc_in-m-pv-)
- [filterkv](#notifyfilterkv-m-f-a-)
- [as_sync](#notifyas_async-f-a-)

<br>

## Private methods
Documentation about the inner-workings of pgnotifier is kept separately from the
README, and can be found over here: [Private methods](./private_methods.md). or
via the method links below.

**[Private methods](./private_methods.md)**
- [__maybe_stop](./private_methods.md#notifier__maybe_stop-)
- [__maybe_restart](./private_methods.md#notifier__maybe_restart-)
- [__notify](./private_methods.md#notifier__notify)
- [__valid_chans](./private_methods.md#notifier__valid_chans)
- [__mute_chans](./private_methods.md#notifier__mute_chans-channels-b-)
- [__mute_sub](./private_methods.md#notifier__mute_sub-id-channels-b-)
<br>
<br>
<br>


## TODO
A list of stuff to look into at a later date can be found over here:
[TODO](./TODO.md)



## API
The methods below provide everything needed to work with pgnotifier.

#### <mark><strong>Notifier( <em style="font-weight:400">dbconf</em> )</strong></mark>
Constructor.

Args:
 * `dbconf` database configuration, as `dict`.


``` python
from pgnotifier import Notifier

n = Notifier(dbconf)
```
<hr style="height:1px">



#### <mark><strong><a style="font-weight:400">Notifier</a>.add_channels( <em style="font-weight:400">channels</em> )</strong></mark>
Adds one or more channels to the set of channels to monitor.
Is a no-op if channel already exists.

Args:
* `channels` list of channels to add, as `str` (single channel), `list` or `set`.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
n.add_channels(['ch1', 'ch2', 'ch3'])
```
<hr style="height:1px">


#### <mark><strong><a style="font-weight:400">Notifier</a>.remove_channels( <em style="font-weight:400">channels, autorun=True</em> )</strong></mark>
Removes one or more channels from the set of channels to monitor.
Is a no-op if channel doesn't exist. Optionally restarts listener thread (if needed).

Args:
* `channels` list of channels to remove, as `str` (single channel), `list` or `set`.
* `autorun` restart listener thread with channels removed, as `bool`. Defaults to `True`.

<br>

> [!IMPORTANT]
> Active channels, when removed, *will only* cease being monitored after a
listener thread restart. Thread restarts happen automatically when `autorun=True`.
Otherwise, if `autorun=False`, removed channels *will* continue to be
monitored until a call to `stop()` and `start()`, or `restart()`, is made.
>
> Inactive channels (e.g. channel is muted and/or has no subscribers and/or
has all muted subscribers), when removed, *do not* require a restart as
they will have already been removed from the listener thread.
>
> It's advisable to allow pgnotifier take care of listener thread management
via the default `autorun=True`, *unless there is a very good reason* to
manage it manually.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.remove_channels('ch2')
c = n.channels()
print("channels:", c)
```

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.channels( )</strong></mark>
Returns channel and subscriber data, as `dict`.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
c = n.channels()
print("channels: ", c)
```

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.subscribe( <em style="font-weight:400">id, channel, fn, autorun=True</em> )</strong></mark>
Adds a callback function with id for notifications on channel.
Creates channel if channel does not exist.
Optionally restarts listener thread (if needed).

Args:
* `id` subscriber id, as `hashable` (i.e. any immutable type such as
strings, numbers, and tuples containing immutable types).
* `channel` notification channel to subscribe to, as `str`.
* `fn` callback function, as `callable` (i.e. function or method).
* `autorun` restart listener thread (if needed), as `bool`. Defaults to `True`.

> [!IMPORTANT]
> A new channel, when added with this subscriber, or, a channel that becomes
active due to this subscriber *can only* be monitored after a
listener thread restart. Thread restarts happen automatically when `autorun=True`.
Otherwise, if `autorun=False`, activated channels containing this subscriber
*will not* be monitored until a call to `stop()` and `start()`, or `restart()`,
is made.
>
> It's advisable to allow pgnotifier take care of listener thread management
via the default `autorun=True`, *unless there is a very good reason* to
manage it manually.

When a notification is received on a channel, callbacks subscribed to that channel
will be executed.

Args:
* `id` the subscriber `id` as `hashable`.
* `channel` the notification channel, as `str`.
* `payload` the notification received, as native type as cast by `ast.literal_eval`.
* `pid` the notifying sessions server process PID, as `int`.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
n.subscribe(42, 'ch4',
    lambda id, channel, payload, pid: print("id: ", id, ", channel: ", channel,
        ", payload: ", payload, ", pid: ", pid))
```

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.unsubscribe( <em style="font-weight:400">id, channel, autorun=True</em> )</strong></mark>
Removes a callback function with id from notifications on channel.
Optionally restarts listener thread (if needed).

Args:
* `id` the subscriber id, as `hashable`.
* `channel` notification channel to unsubscribe from, as `str`.
* `autorun` restart listener thread (if needed), as `bool`. Defaults to `True`.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.unsubscribe(42, 'ch1')
```
<hr style="height:1px">


#### <mark><strong><a style="font-weight:400">Notifier</a>.subscribers( )</strong></mark>
Returns subscriber and channel data, as `dict`.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
s = n.subscribers()
print("subscribers:", s)

```

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.mute_channels( <em style="font-weight:400">channels=pyrsistent.PVector</em> )</strong></mark>
Mutes channels. Removes channels from listener thread, thereby muting all
subscribers associated with those channels (no matter their mute status).

Subscribers will retain their mute status associated with those channels.

Args:
* `channels` list of channels to mute, as `str` (single channel), `list` or `set`.
If no channels given, *ALL* channels will be muted.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.mute_channels('ch1')
m = n.muted_channels()
print("muted channels:", m)
```

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.unmute_channels( <em style="font-weight:400">channels=pyrsistent.PVector</em> )</strong></mark>
Un-mutes channels. Adds channels to the listener thread, thereby adding all
un-muted subscribers associated with those channels.

Args:
* `channels` list of channels to un-mute, as `str` (single channel), `list` or `set`.
If no channels given, *ALL* channels will be un-muted.

> [!NOTE]
> Channel will remain inactive (i.e. excluded from the listener thread) if it
*does not* contain any non-muted subscribers.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.unmute_channels()
m = n.non_muted_channels()
print("non muted channels:", m)
```

<hr style="height:1px">


#### <mark><strong><a style="font-weight:400">Notifier</a>.mute_subscriber( <em style="font-weight:400">id, channels=pyrsistent.PVector</em> )</strong></mark>
Mutes subscriber on channels. If a channel no longer contains any non-muted
subscribers, it is said to be *inactive* and is removed from the listener thread.

Args:
* `id` subscriber id, as `hashable` (i.e. any immutable type such as
strings, numbers, and tuples containing immutable types).
* `channels` list of channels to mute the subscriber on, as `str`
(single channel), `list` or `set`.
If no channels given, the subscriber will be muted on *ALL* channels it is
subscribed to.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.mute_subscriber('an_id', 'ch2')
m = n.muted_subscribers()
print("muted subscribers:", m)
```

<hr style="height:1px">


#### <mark><strong><a style="font-weight:400">Notifier</a>.unmute_subscriber( <em style="font-weight:400">id, channels=pyrsistent.PVector</em> )</strong></mark>
Un-mutes subscriber on channels. If subscriber is on a non-muted, *inactive*
channel, the channel becomes *active* and is added to the listener thread.

Args:
* `id` subscriber id, as `hashable` (i.e. any immutable type such as
strings, numbers, and tuples containing immutable types).
* `channels` list of channels to un-mute the subscriber on, as `str`
(single channel), `list` or `set`.
If no channels given, the subscriber will be unmuted on *ALL* channels it is
subscribed to.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added, removed, etc. ...
n.unmute_subscriber('an_id')
m = n.muted_subscribers()
print("muted subscribers:", m)
```

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.start( )</strong></mark>
Starts the listener thread (if not already running).
Is a no-op if thread already running.
*This function is generally not needed in userland.*

> [!NOTE]
> Listener thread (re)starts are only required under certain, specific circumstances.
See [__maybe_restart](./private_methods.md#notifier__maybe_restart-) for more detail.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added removed, etc. ...
n.start()
```

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.stop( )</strong></mark>
Stops the listener thread (if running). Is a no-op if thread is not running.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been added removed, etc. ...
n.stop()
```

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.restart( )</strong></mark>
(Re)starts listener thread.
*This function is generally not needed in userland.*

> [!NOTE]
> Listener thread (re)starts are only required under certain, specific circumstances.
See [__maybe_restart](./private_methods.md#notifier__maybe_restart-) for more detail.

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been changed with arg autorun=False ...
n.restart()
```
<hr style="height:1px">


#### <mark><strong><a style="font-weight:400">Notifier</a>.is_running( )</strong></mark>
Returns True if listener thread currently running, else False, as `bool`

``` python
from pgnotifier import Notifier

n = Notifier(conf)
# channels and/or subscribers, have been changed with arg autorun=False ...
b = n.is_running()
print("listener running?",b)
```

<br>

## Internal helper functions
The functions below are not required outside the internals of pgnotifier. They
are publicly exposed and included here as a matter of interest.


#### <mark><strong><a style="font-weight:400">notify</a>.assoc_in( <em style="font-weight:400">m, pv, v</em> )</strong></mark>
A clojure-esque nested associative map transformer for [Pyrsistent](https://github.com/tobgu/pyrsistent). Associates a new value `v` at key path `pv` in map `m`. Returns a new map with associated changes, as `pyrsistent.PMap`.

Args:
* `m` map to transform, as `pyrsistent.PMap`.
* `pv` a path vector of keys indicating location of desired assoc, as `pyrsistent.PVector` or `list`
* `v` new value to assoc into path given by `pv`, as `whatever!`

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">notify</a>.dissoc_in( <em style="font-weight:400">m, pv</em> )</strong></mark>
Nested associative map key->val remover for [Pyrsistent](https://github.com/tobgu/pyrsistent). Returns a new map with dissociated changes, as `pyrsistent.PMap`.

Args:
* `m` map to transform, as `pyrsistent.PMap`.
* `pv` a path vector of keys indicating location of desired dissoc, as `pyrsistent.PVector` or `list`

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">notify</a>.filterkv( <em style="font-weight:400">m, f, *a</em> )</strong></mark>
Trivial associative map filter. Returns a new map with filtered changes, as `pyrsistent.PMap`.

Args:
* `m` map to filter, as `pyrsistent.PMap`.
* `f` filter function that accepts at least a key and a value as args, as `callable`.
* `*a` optional additional args to pass to filter function, as `whatever!`

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">notify</a>.as_async( <em style="font-weight:400">f, *a</em> )</strong></mark>
Runs asynchronous and/or blocking functions in a new asyncio loop, as a task, in a thread. Designed to be called from a synchronous context. Returns `concurrent.futures.Future`.

Args:
* `f` function to run in asyncio loop, as `callable`.
* `*a` optional args to function `f` (e.g. a blocking function call that might produce a result in the future), as `whatever!`

<hr style="height:1px">
