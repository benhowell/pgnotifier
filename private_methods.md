# pgnotifier
A simple little module to capture, process, and dispatch Postgresql NOTIFY streams
<br>
## Private methods
The methods below are private and not intended for use. Documentation
is included here to detail the inner-workings as a matter of completeness.

#### <mark><strong><a style="font-weight:400">Notifier</a>.__maybe_stop( )</strong></mark>
Stops the listener thread (if running) and closes the database connection.
Is a no-op if thread is not running.

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.__maybe_restart( )</strong></mark>
Restarts listener thread if active channels have been deemed inactive or
inactive channels have been deemed active (i.e. there's a reason to add
and/or remove channels to/from the listener thread).

Listener thread restart process:
* Valid channels (those that should be active) are compared to the active channels
(those in the listener thread).
* If thread not running, a restart with valid channels is required.
* If thread running and valid channels don't match active channels, a restart is required.
* Listener thread stopped if running:
  * Task thread is terminated via `cancel()` on enclosing asyncio loop.
  * Database cursor and connection are closed.
* New database connection is created.
* Postgresql LISTEN commands are executed (one per active channel)
* NOTIFY message callback function, and `psycopg.connection.notifies()`
call (which returns a blocking generator) are passed to the asyncio loop,
which in-turn is executed as a task inside a thread returning a Future.
Whenever the future returns NOTIFY data received from Postgresql, the
NOTIFY message callback distributes that data to all callbacks
subscribed to the channel the data arrived on.

Whenever the future returns NOTIFY data received from Postgresql, the
NOTIFY message callback distributes that data to all callbacks
subscribed to the channel the data arrived on.

> [!NOTE]
> The listener thread is started when Notifier is first constructed. The
listener thread is _always_ running and always listening to at least
one channel (the SYSCHAN: PGNOTIFIER), unless `stop()` has been called
from userland.

SYSCHAN needs further consideration. I expect a future release to
properly integrate the SYSCHAN with the functionality of a regular
channel with the exception that it is always on the listener thread.
Perhaps system commands can be executed via messages from the channel?
Postgresql system messages? Other non-application level stuff?

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.__notify( <em style="font-weight:400">generator</em> )</strong></mark>
Receives incoming NOTIFY message data from all active channels.
The generator arg yields each message as a string, as it arrives.

Payload is cast to it's native data type via `ast.literal_eval` and distributed
to callbacks subscribed to the channel the message arrived on.

Args:
* `generator` message generator

> [!IMPORTANT]
> Messages must be shorter than 8000 bytes. For almost all notifications, it's
recommended to send the key of record, a view or table name, a function reference, etc.

Subscribers to channel will have their callbacks executed with args:
* `id`  the id of the subscriber
* `channel` the notification channel
* `payload` the notification received
* `pid` the notifying sessions server process PID

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.__valid_chans( )</strong></mark>
Returns a vector of channels deemed valid to be on the listener thread, as `pyrsistent.PVector`.

A channel is deemed to be valid if:
* It is not muted.
* It contains at least one non-muted subscriber.

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.__mute_chans( <em style="font-weight:400">channels, b</em> )</strong></mark>
Sets the mute state `b` to channels (i.e. will be removed from the listener thread).

All subscribers associated with muted channels are also removed from the
listener thread (no matter their mute status). Subscribers will retain
their mute status associated with those channels.

If a channel is unmuted, the subscribers to that channel will resume
operation according to their mute status.

Args:
* `channels` list of channels to set mute state on, as `str` (single channel),
`list` or `set`. If no channels given, *ALL* channels will be muted.
* `b` boolean value to set mute to, as `bool`

<hr style="height:1px">

#### <mark><strong><a style="font-weight:400">Notifier</a>.__mute_sub( <em style="font-weight:400">id, channels, b</em> )</strong></mark>
Sets the mute state `b` of subscriber with id on channels.

Args:
* `id` subscriber id, as `hashable` (i.e. any immutable type such as strings,
numbers, and tuples containing immutable types).
* `channels` list of channels to the subscriber mute state on, as `str`
(single channel), `list` or `set`. If no channels given, subscriber with `id`
will have it's mute state set to `b` on *ALL* channels it is subscribed to.

<hr style="height:1px">
